//! Background task. It consumes the state topic from offset 0, tracks the
//! latest non-tombstone value, and flips `LoadedState::is_loaded` once the
//! consumer has seen no new record for 5 consecutive 100ms polls. That is the
//! "quiet period" end-of-log heuristic.

use std::sync::Arc;

use krabka_client_core::Client;
use krabka_protocol::{
    owned::{
        fetch_request::{FetchPartition, FetchRequest, FetchTopic},
        fetch_response::FetchResponse,
    },
    primitives::uuid::Uuid,
    records::RecordsPayload,
};
use krabka_units::{
    ByteSize, Time,
    convert::{ByteSizeExt as _, TimeExt as _},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::{
    config::RebalancerRuntimePolicy,
    state_topic::{
        LoadedState, STATE_KEY,
        error::{StateTopicError, is_transient_topic_partition_code},
        route::{TopicRoute, resolve_topic_route},
        serde_format,
    },
};
/// The loader drains the log as fast as the broker answers, so it asks for
/// whatever is already there instead of parking on the broker's fetch queue.
const NO_FETCH_WAIT: Time = Time::ZERO;
const NO_MIN_BYTES: ByteSize = ByteSize::ZERO;

/// `(absolute_offset, key_bytes, value_bytes)`. The value is `None` for
/// tombstones.
type FetchedRecord = (i64, Option<Vec<u8>>, Option<Vec<u8>>);

pub struct StateTopicLoader {
    pub client: Arc<Client>,
    pub topic: String,
    pub state: Arc<LoadedState>,
    pub shutdown: CancellationToken,
    pub runtime_policy: RebalancerRuntimePolicy,
}

impl StateTopicLoader {
    pub async fn run(self) {
        info!(topic = %self.topic, "state-topic loader started");
        let mut next_offset: i64 = 0;
        let mut quiet_polls: usize = 0;
        let mut route: Option<TopicRoute> = None;
        loop {
            tokio::select! {
                () = tokio::time::sleep(self.runtime_policy.state_loader_poll_interval.to_std()) => {}
                () = self.shutdown.cancelled() => {
                    info!("state-topic loader shutting down");
                    return;
                }
            }
            let current = match route {
                Some(current) => current,
                None => match resolve_topic_route(&self.client, &self.topic).await {
                    Ok(Some(resolved)) => *route.insert(resolved),
                    Ok(None) => {
                        debug!(topic = %self.topic, "metadata has no state-topic id or leader yet; will retry");
                        continue;
                    }
                    Err(e) => {
                        debug!(error = %e, "state-topic metadata failed; will retry");
                        continue;
                    }
                },
            };
            match self.poll_once(current, next_offset).await {
                Ok(records) => {
                    let saw_new = apply_fetched_records(&self.state, &mut next_offset, records);
                    if saw_new {
                        quiet_polls = 0;
                    } else {
                        quiet_polls += 1;
                        if should_mark_loaded(
                            quiet_polls,
                            self.runtime_policy.state_loader_quiet_polls.get(),
                            self.state.is_loaded(),
                        ) {
                            info!("state-topic load reached steady state; marking loaded");
                            self.state.mark_loaded();
                        }
                    }
                }
                Err(e) => {
                    debug!(error = %e, "state-topic poll failed; will re-resolve the route and retry");
                    // Do NOT advance offset; do NOT count as quiet. The
                    // failure may mean the topic was recreated under a new
                    // id (UNKNOWN_TOPIC_ID) or its leader moved
                    // (NOT_LEADER_OR_FOLLOWER), so resolve both again.
                    route = None;
                }
            }
        }
    }

    async fn poll_once(
        &self,
        route: TopicRoute,
        fetch_offset: i64,
    ) -> Result<Vec<FetchedRecord>, StateTopicError> {
        let req = fetch_request_with_max(
            &self.topic,
            route.topic_id,
            fetch_offset,
            self.runtime_policy.state_fetch_max,
        );
        let resp = self.client.broker(route.leader_id).send(req).await?;
        fetched_records_from_response(&resp)
    }
}

#[cfg(test)]
fn fetch_request(topic: &str, topic_id: Uuid, fetch_offset: i64) -> FetchRequest {
    fetch_request_with_max(
        topic,
        topic_id,
        fetch_offset,
        RebalancerRuntimePolicy::default().state_fetch_max,
    )
}

/// Build the loader's Fetch for partition 0 of the state topic.
///
/// The request names the topic twice because Kafka's Fetch schema does:
/// v0-v12 carry only `topic`, and v13+ (KIP-516) carry only `topic_id`.
/// `KafkaApis.handleFetchRequest` resolves a v13+ id through the metadata
/// cache and answers `UNKNOWN_TOPIC_ID` for every partition of an id it
/// cannot resolve, so the id must be the topic's real one.
fn fetch_request_with_max(
    topic: &str,
    topic_id: Uuid,
    fetch_offset: i64,
    fetch_max: ByteSize,
) -> FetchRequest {
    FetchRequest {
        max_wait_ms: NO_FETCH_WAIT.millis_i32(),
        min_bytes: NO_MIN_BYTES.bytes_i32(),
        max_bytes: fetch_max.bytes_i32(),
        topics: vec![FetchTopic {
            topic: topic.to_string(),
            topic_id,
            partitions: vec![FetchPartition {
                partition: 0,
                fetch_offset,
                partition_max_bytes: fetch_max.bytes_i32(),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn should_mark_loaded(quiet_polls: usize, required_quiet_polls: usize, is_loaded: bool) -> bool {
    quiet_polls >= required_quiet_polls && !is_loaded
}

fn apply_fetched_records(
    state: &LoadedState,
    next_offset: &mut i64,
    records: Vec<FetchedRecord>,
) -> bool {
    let saw_new = !records.is_empty();
    for (offset, key, value) in records {
        *next_offset = offset + 1;
        if key.as_deref() != Some(STATE_KEY.as_bytes()) {
            continue; // ignore unknown keys
        }
        match value {
            None => state.store(None),
            Some(bytes) => match serde_format::decode(&bytes) {
                Ok(f) => state.store(Some(f)),
                Err(e) => {
                    warn!(
                        error = %e,
                        offset,
                        "state-topic record had malformed JSON; skipping"
                    );
                }
            },
        }
    }
    saw_new
}

fn fetched_records_from_response(
    resp: &FetchResponse,
) -> Result<Vec<FetchedRecord>, StateTopicError> {
    let mut out: Vec<FetchedRecord> = Vec::new();
    for t in &resp.responses {
        for p in &t.partitions {
            if p.error_code != 0 {
                if is_transient_topic_partition_code(p.error_code) {
                    // Transient: topic/partition not yet visible to this
                    // broker. Treat as empty — the caller counts it as a
                    // quiet poll.
                    debug!(
                        error_code = p.error_code,
                        "state-topic fetch: transient partition error; treating as empty"
                    );
                    continue;
                }
                return Err(StateTopicError::FetchErrorCode { code: p.error_code });
            }
            let Some(payload) = &p.records else { continue };
            let RecordsPayload::V2(batches) = payload else {
                continue;
            };
            for batch in batches {
                for r in &batch.records {
                    let off = batch.base_offset + i64::from(r.offset_delta);
                    out.push((
                        off,
                        r.key.as_ref().map(|b| b.to_vec()),
                        r.value.as_ref().map(|b| b.to_vec()),
                    ));
                }
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use krabka_protocol::{
        UnknownTaggedFields,
        owned::{
            fetch_request::ReplicaState,
            fetch_response::{FetchResponse, FetchableTopicResponse, PartitionData},
        },
        primitives::uuid::Uuid,
        records::{Record, RecordBatch, RecordsPayload},
    };
    use krabka_units::millis;

    use super::*;
    use crate::{
        executor::state::{InFlightFile, Phase},
        state_topic::test_broker::{Answer, Seen, TestBroker},
    };

    /// Connect and request timeout for the deliberately unreachable test
    /// client.
    const CLIENT_TIMEOUT: Time = millis(50);

    fn in_flight(id: &str) -> InFlightFile {
        InFlightFile::new(
            id.to_string(),
            Phase::Wait,
            42,
            krabka_units::bytes_per_sec(50_000_000),
        )
    }

    fn fetched(offset: i64, key: Option<&str>, value: Option<Vec<u8>>) -> FetchedRecord {
        (offset, key.map(|s| s.as_bytes().to_vec()), value)
    }

    fn fetch_response(error_code: i16, records: Option<RecordsPayload>) -> FetchResponse {
        FetchResponse {
            responses: vec![FetchableTopicResponse {
                partitions: vec![PartitionData {
                    error_code,
                    records,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    #[test]
    fn fetch_request_targets_state_topic_partition_with_consumer_limits() {
        let req = fetch_request("__krabka_state", Uuid([7; 16]), 123);
        assert2::assert!(
            req == FetchRequest {
                replica_id: -1,
                max_wait_ms: 0,
                min_bytes: 0,
                max_bytes: 1_048_576,
                isolation_level: 0,
                session_id: 0,
                session_epoch: -1,
                topics: vec![FetchTopic {
                    topic: "__krabka_state".into(),
                    topic_id: Uuid([7; 16]),
                    partitions: vec![FetchPartition {
                        partition: 0,
                        current_leader_epoch: -1,
                        fetch_offset: 123,
                        last_fetched_epoch: -1,
                        log_start_offset: -1,
                        partition_max_bytes: 1_048_576,
                        replica_directory_id: Uuid([0; 16]),
                        high_watermark: i64::MAX,
                        unknown_tagged_fields: UnknownTaggedFields(vec![]),
                    }],
                    unknown_tagged_fields: UnknownTaggedFields(vec![]),
                }],
                forgotten_topics_data: vec![],
                rack_id: String::new(),
                cluster_id: None,
                replica_state: ReplicaState {
                    replica_id: -1,
                    replica_epoch: -1,
                    unknown_tagged_fields: UnknownTaggedFields(vec![]),
                },
                unknown_tagged_fields: UnknownTaggedFields(vec![]),
            }
        );
    }

    #[test]
    fn fetch_request_uses_custom_maximum() {
        let request = fetch_request_with_max(
            "__krabka_state",
            Uuid([7; 16]),
            0,
            krabka_units::kibibytes(32),
        );
        assert2::assert!(request.max_bytes == 32 * 1024);
        assert2::assert!(request.topics[0].partitions[0].partition_max_bytes == 32 * 1024);
    }

    #[test]
    fn should_mark_loaded_only_at_quiet_threshold_before_loaded() {
        for (quiet_polls, is_loaded, want) in
            [(4, false, false), (5, false, true), (5, true, false)]
        {
            assert2::assert!(should_mark_loaded(quiet_polls, 5, is_loaded) == want);
        }
    }

    #[test]
    fn applying_records_stores_state_key_and_advances_offset() {
        let state = LoadedState::new();
        let mut next_offset = 0;
        let value = serde_format::encode(&in_flight("p-1")).unwrap().to_vec();

        apply_fetched_records(
            &state,
            &mut next_offset,
            vec![fetched(4, Some(STATE_KEY), Some(value))],
        );

        assert2::assert!(next_offset == 5);
        assert2::assert!(state.current().is_some_and(|f| f.proposal_id == "p-1"));
    }

    #[test]
    fn applying_records_advances_offset_over_unknown_key_without_changing_state() {
        let state = LoadedState::new();
        let existing = in_flight("existing");
        state.store(Some(existing.clone()));
        let mut next_offset = 9;

        apply_fetched_records(
            &state,
            &mut next_offset,
            vec![fetched(9, Some("other-key"), Some(b"ignored".to_vec()))],
        );

        assert2::assert!(next_offset == 10);
        assert2::assert!(state.current().is_some_and(|f| f.proposal_id == "existing"));
    }

    #[test]
    fn applying_tombstone_clears_state_and_advances_offset() {
        let state = LoadedState::new();
        state.store(Some(in_flight("existing")));
        let mut next_offset = 0;

        apply_fetched_records(
            &state,
            &mut next_offset,
            vec![fetched(7, Some(STATE_KEY), None)],
        );

        assert2::assert!(next_offset == 8);
        assert2::assert!(state.current().is_none());
    }

    #[test]
    fn fetch_response_non_transient_error_is_returned() {
        let err = fetched_records_from_response(&fetch_response(42, None)).unwrap_err();
        assert2::assert!(matches!(err, StateTopicError::FetchErrorCode { code: 42 }));
    }

    #[test]
    fn fetch_response_extracts_absolute_offsets_from_batches() {
        let payload = RecordsPayload::V2(vec![RecordBatch {
            base_offset: 10,
            records: vec![
                Record {
                    offset_delta: 0,
                    key: Some(Bytes::from_static(STATE_KEY.as_bytes())),
                    value: Some(Bytes::from_static(b"one")),
                    ..Default::default()
                },
                Record {
                    offset_delta: 2,
                    key: Some(Bytes::from_static(STATE_KEY.as_bytes())),
                    value: Some(Bytes::from_static(b"two")),
                    ..Default::default()
                },
            ],
            ..Default::default()
        }]);

        let records = fetched_records_from_response(&fetch_response(0, Some(payload))).unwrap();

        assert2::assert!(
            records
                == vec![
                    (
                        10,
                        Some(STATE_KEY.as_bytes().to_vec()),
                        Some(b"one".to_vec())
                    ),
                    (
                        12,
                        Some(STATE_KEY.as_bytes().to_vec()),
                        Some(b"two".to_vec())
                    ),
                ]
        );
    }

    #[tokio::test]
    async fn poll_once_propagates_fetch_send_errors() {
        let client = Arc::new(
            Client::builder()
                .bootstrap("127.0.0.1:1")
                .client_id("state-topic-loader-test")
                .connect_timeout(CLIENT_TIMEOUT)
                .request_timeout(CLIENT_TIMEOUT)
                .build()
                .await
                .expect("client build does not connect"),
        );
        let loader = StateTopicLoader {
            client,
            topic: "__krabka_state".into(),
            state: LoadedState::new(),
            shutdown: CancellationToken::new(),
            runtime_policy: RebalancerRuntimePolicy::default(),
        };

        let route = TopicRoute {
            topic_id: Uuid([7; 16]),
            leader_id: 1,
        };

        assert2::assert!(loader.poll_once(route, 0).await.is_err());
    }

    /// Kafka's `UNKNOWN_TOPIC_ID` error code.
    const UNKNOWN_TOPIC_ID: i16 = 100;
    const STATE_TOPIC: &str = "__krabka_state";
    /// How long a loader under test may take to mark the state loaded.
    const LOAD_DEADLINE: std::time::Duration = std::time::Duration::from_secs(10);

    fn fast_policy() -> RebalancerRuntimePolicy {
        RebalancerRuntimePolicy {
            state_loader_poll_interval: millis(1),
            ..Default::default()
        }
    }

    fn fetch_ids(broker: &TestBroker) -> Vec<Uuid> {
        broker
            .seen
            .lock()
            .unwrap()
            .iter()
            .filter_map(|seen| match seen {
                Seen::Fetch(_, request) => Some(request.topics[0].topic_id),
                Seen::Produce(..) => None,
            })
            .collect()
    }

    fn spawn_loader(broker: &TestBroker) -> (Arc<LoadedState>, CancellationToken) {
        let state = LoadedState::new();
        let shutdown = CancellationToken::new();
        tokio::spawn(
            StateTopicLoader {
                client: Arc::clone(&broker.client),
                topic: STATE_TOPIC.into(),
                state: Arc::clone(&state),
                shutdown: shutdown.clone(),
                runtime_policy: fast_policy(),
            }
            .run(),
        );
        (state, shutdown)
    }

    async fn wait_until(what: &str, mut done: impl FnMut() -> bool) {
        tokio::time::timeout(LOAD_DEADLINE, async {
            while !done() {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {what}"));
    }

    #[tokio::test]
    async fn loader_fetches_by_topic_id_at_the_negotiated_version() {
        let topic_id = Uuid([7; 16]);
        let value = serde_format::encode(&in_flight("p-1")).unwrap();
        let broker = TestBroker::start(
            STATE_TOPIC,
            topic_id,
            Box::new(move |seen| {
                let Seen::Fetch(_, request) = seen else {
                    panic!("loader only fetches");
                };
                // `KafkaApis.handleFetchRequest` refuses an id that does not
                // resolve, and the zero id never does.
                if request.topics[0].topic_id != topic_id {
                    return Answer::Fetch(fetch_response(UNKNOWN_TOPIC_ID, None));
                }
                let records = (request.topics[0].partitions[0].fetch_offset == 0).then(|| {
                    RecordsPayload::V2(vec![RecordBatch {
                        records: vec![Record {
                            key: Some(Bytes::from_static(STATE_KEY.as_bytes())),
                            value: Some(value.clone()),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }])
                });
                Answer::Fetch(fetch_response(0, records))
            }),
        )
        .await;

        let (state, shutdown) = spawn_loader(&broker);
        wait_until("the state to load", || state.is_loaded()).await;
        shutdown.cancel();

        // Fetch v18 carries the topic id and no name, so the decoded name is
        // empty.
        let wire_topic = |fetch_offset| {
            let mut request = fetch_request(STATE_TOPIC, topic_id, fetch_offset);
            request.topics[0].topic = String::new();
            request
        };
        let seen = broker.seen.lock().unwrap().clone();
        assert2::assert!(
            seen[..2]
                == [
                    Seen::Fetch(18, wire_topic(0)),
                    Seen::Fetch(18, wire_topic(1))
                ]
        );
        assert2::assert!(state.current().is_some_and(|f| f.proposal_id == "p-1"));
    }

    #[tokio::test]
    async fn loader_resolves_the_topic_id_again_after_unknown_topic_id() {
        let old_id = Uuid([1; 16]);
        let new_id = Uuid([2; 16]);
        let broker = TestBroker::start(
            STATE_TOPIC,
            old_id,
            Box::new(move |seen| {
                let Seen::Fetch(_, request) = seen else {
                    panic!("loader only fetches");
                };
                // Only the recreated topic's id resolves.
                let code = if request.topics[0].topic_id == new_id {
                    0
                } else {
                    UNKNOWN_TOPIC_ID
                };
                Answer::Fetch(fetch_response(code, None))
            }),
        )
        .await;

        let (state, shutdown) = spawn_loader(&broker);
        wait_until("a fetch by the stale id", || !fetch_ids(&broker).is_empty()).await;
        assert2::assert!(!state.is_loaded());
        *broker.topic_id.lock().unwrap() = new_id;
        wait_until("the state to load", || state.is_loaded()).await;
        shutdown.cancel();

        let mut ids = fetch_ids(&broker);
        ids.dedup();
        assert2::assert!(ids == [old_id, new_id]);
    }
}
