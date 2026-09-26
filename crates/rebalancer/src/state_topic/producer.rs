//! Single-key produce path for the state topic. It is built directly on
//! `crabka_client_core::Client`, to match the rebalancer's
//! `ingest::admin_client` pattern. A one-key-per-write workload does not need
//! the high-level `krabka-client-producer`.

use bytes::Bytes;
use crabka_client_core::Client;
use crabka_protocol::{
    owned::{
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
        produce_response::ProduceResponse,
    },
    primitives::uuid::Uuid,
    records::{Record, RecordBatch},
};
use crabka_units::convert::TimeExt as _;
use tracing::debug;

use crate::{
    config::RebalancerRuntimePolicy,
    state_topic::{
        error::{StateTopicError, is_transient_topic_partition_code},
        route::{TopicRoute, resolve_topic_route},
    },
};

/// Produce a single record to `(topic, partition=0)`. `value=None` is a
/// tombstone with a null value, which matches Kafka compaction semantics.
///
/// This uses `acks=all`. The timeout and the transient-error retry policy come
/// from the validated runtime policy.
pub(crate) async fn produce_state(
    client: &Client,
    topic: &str,
    key: &str,
    value: Option<Bytes>,
    policy: &RebalancerRuntimePolicy,
) -> Result<(), StateTopicError> {
    let key_bytes = Bytes::copy_from_slice(key.as_bytes());
    let mut last_transient: Option<i16> = None;
    for attempt in 0..policy.state_produce_retry_attempts.get() {
        // KIP-516: Produce v13+ keys partition routing by `topic_id`.
        // Resolve it via Metadata on each attempt — also nudges the
        // broker to load the topic into its data plane if it hasn't
        // yet, which addresses the post-create transient window.
        let TopicRoute {
            topic_id,
            leader_id,
        } = match resolve_topic_route(client, topic).await {
            Ok(Some(route)) => route,
            Ok(None) => {
                last_transient = Some(3);
                debug!(
                    attempt,
                    topic, "metadata returned no topic_id; retrying after backoff"
                );
                tokio::time::sleep(policy.state_produce_retry_backoff.to_std()).await;
                continue;
            }
            Err(e) => return Err(e),
        };
        match classify_send_result(
            send_once(
                client,
                leader_id,
                topic,
                topic_id,
                &key_bytes,
                value.clone(),
                policy.state_produce_timeout,
            )
            .await,
        ) {
            Ok(None) => return Ok(()),
            Ok(Some(code)) => {
                last_transient = Some(code);
                debug!(
                    code,
                    attempt, "transient produce error; retrying after backoff"
                );
                tokio::time::sleep(policy.state_produce_retry_backoff.to_std()).await;
            }
            Err(e) => return Err(e),
        }
    }
    Err(StateTopicError::ProduceErrorCode {
        code: last_transient.unwrap_or(0),
    })
}

async fn send_once(
    client: &Client,
    leader_id: i32,
    topic: &str,
    topic_id: Uuid,
    key: &Bytes,
    value: Option<Bytes>,
    produce_timeout: crabka_units::Time,
) -> Result<(), StateTopicError> {
    let req = produce_request(topic, topic_id, key, value, produce_timeout);
    let resp = client.broker(leader_id).send(req).await?;
    if let Some(code) = produce_response_error(&resp) {
        return Err(StateTopicError::ProduceErrorCode { code });
    }
    Ok(())
}

fn produce_request(
    topic: &str,
    topic_id: Uuid,
    key: &Bytes,
    value: Option<Bytes>,
    produce_timeout: crabka_units::Time,
) -> ProduceRequest {
    let record = Record {
        key: Some(key.clone()),
        value,
        ..Default::default()
    };
    let batch = RecordBatch {
        records: vec![record],
        ..Default::default()
    };
    ProduceRequest {
        acks: -1, // all
        timeout_ms: produce_timeout.millis_i32(),
        topic_data: vec![TopicProduceData {
            name: topic.into(),
            topic_id,
            partition_data: vec![PartitionProduceData {
                index: 0,
                records: Some(batch.into()),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn produce_response_error(resp: &ProduceResponse) -> Option<i16> {
    for t in &resp.responses {
        for p in &t.partition_responses {
            if p.error_code != 0 {
                return Some(p.error_code);
            }
        }
    }
    None
}

/// Kafka's `UNKNOWN_TOPIC_ID` error code. Produce v13+ answers it when the
/// request's topic id does not resolve: the topic was recreated under a new
/// id, or the leader has not yet applied the topic. The Java client treats it
/// as retriable metadata staleness, and `produce_state` resolves the id again
/// on every attempt.
const UNKNOWN_TOPIC_ID: i16 = 100;

fn classify_send_result(
    result: Result<(), StateTopicError>,
) -> Result<Option<i16>, StateTopicError> {
    match result {
        Ok(()) => Ok(None),
        Err(StateTopicError::ProduceErrorCode { code })
            if code == UNKNOWN_TOPIC_ID || is_transient_topic_partition_code(code) =>
        {
            Ok(Some(code))
        }
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::check;
    use crabka_protocol::{
        owned::produce_response::{
            PartitionProduceResponse, ProduceResponse, TopicProduceResponse,
        },
        records::RecordsPayload,
    };
    use crabka_units::{Time, millis, secs};

    use super::*;
    use crate::state_topic::test_broker::{Answer, Seen, TestBroker};

    /// Connect and request timeout for the deliberately unreachable test
    /// client.
    const CLIENT_TIMEOUT: Time = millis(50);

    fn response_with_error(code: i16) -> ProduceResponse {
        ProduceResponse {
            responses: vec![TopicProduceResponse {
                partition_responses: vec![PartitionProduceResponse {
                    error_code: code,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    fn unreachable_client_id(suffix: &str) -> String {
        format!("state-topic-producer-test-{suffix}")
    }

    async fn unreachable_client(suffix: &str) -> Client {
        Client::builder()
            .bootstrap("127.0.0.1:1")
            .client_id(unreachable_client_id(suffix))
            .connect_timeout(CLIENT_TIMEOUT)
            .request_timeout(CLIENT_TIMEOUT)
            .build()
            .await
            .expect("client build does not connect")
    }

    #[test]
    fn produce_request_writes_single_key_record_to_partition_zero() {
        let topic_id = Uuid([7; 16]);
        let key = Bytes::from_static(b"in_flight");
        let value = Some(Bytes::from_static(b"{json}"));

        let req = produce_request("state-topic", topic_id, &key, value.clone(), secs(10));

        check!(
            (
                req.transactional_id.as_ref(),
                req.acks,
                req.timeout_ms,
                req.topic_data.first().map(|topic| {
                    (
                        topic.name.as_str(),
                        topic.topic_id,
                        topic
                            .partition_data
                            .first()
                            .map(|partition| partition.index),
                    )
                }),
            ) == (None, -1, 10_000, Some(("state-topic", topic_id, Some(0))))
        );
        let records = req.topic_data[0].partition_data[0]
            .records
            .as_ref()
            .expect("records");
        let RecordsPayload::V2(batches) = records else {
            panic!("produce request should use v2 record batches");
        };
        check!(
            batches
                .iter()
                .flat_map(|batch| &batch.records)
                .map(|record| (record.key.as_ref(), &record.value))
                .collect::<Vec<_>>()
                == vec![(Some(&key), &value)]
        );
    }

    #[test]
    fn produce_response_errors_are_classified_for_retry() {
        check!(classify_send_result(Ok(())).unwrap().is_none());
        for code in [3, 5, 9, UNKNOWN_TOPIC_ID] {
            check!(
                classify_send_result(Err(StateTopicError::ProduceErrorCode { code })).unwrap()
                    == Some(code)
            );
        }
        let err =
            classify_send_result(Err(StateTopicError::ProduceErrorCode { code: 42 })).unwrap_err();
        assert2::assert!(matches!(
            err,
            StateTopicError::ProduceErrorCode { code: 42 }
        ));
    }

    #[test]
    fn produce_response_error_scans_partition_responses() {
        for (_name, code, expected) in [
            ("successful partition", 0, None),
            ("failed partition", 42, Some(42)),
        ] {
            assert2::assert!(produce_response_error(&response_with_error(code)) == expected);
        }
    }

    #[tokio::test]
    async fn produce_state_propagates_initial_metadata_send_errors() {
        let client = unreachable_client("produce-state").await;

        assert2::assert!(
            produce_state(
                &client,
                "__krabka_state",
                "in_flight",
                Some(Bytes::from_static(b"{}")),
                &RebalancerRuntimePolicy::default(),
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn send_once_propagates_produce_send_errors() {
        let client = unreachable_client("send-once").await;
        let key = Bytes::from_static(b"in_flight");

        assert2::assert!(
            send_once(
                &client,
                7,
                "__krabka_state",
                Uuid([7; 16]),
                &key,
                Some(Bytes::from_static(b"{}")),
                secs(10),
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn produce_state_retries_unknown_topic_id_with_the_resolved_id() {
        let old_id = Uuid([1; 16]);
        let new_id = Uuid([2; 16]);
        let broker = TestBroker::start(
            "__krabka_state",
            old_id,
            Box::new(move |seen| {
                let Seen::Produce(_, request) = seen else {
                    panic!("producer only produces");
                };
                let error_code = if request.topic_data[0].topic_id == new_id {
                    0
                } else {
                    UNKNOWN_TOPIC_ID
                };
                Answer::Produce(response_with_error(error_code))
            }),
        )
        .await;
        let policy = RebalancerRuntimePolicy {
            state_produce_retry_backoff: millis(1),
            ..Default::default()
        };
        let key = Bytes::from_static(b"in_flight");
        let value = Some(Bytes::from_static(b"{}"));

        // The first attempt names the topic by the id it had before it was
        // recreated; the retry resolves the current one.
        let topic_id = Arc::clone(&broker.topic_id);
        let seen = Arc::clone(&broker.seen);
        tokio::spawn(async move {
            while seen.lock().unwrap().is_empty() {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
            *topic_id.lock().unwrap() = new_id;
        });
        produce_state(
            &broker.client,
            "__krabka_state",
            "in_flight",
            value.clone(),
            &policy,
        )
        .await
        .expect("produce succeeds once the id resolves");

        // Produce v13 carries the topic id and no name, so the decoded name
        // is empty.
        let wire = |topic_id| {
            let mut request =
                produce_request("__krabka_state", topic_id, &key, value.clone(), secs(10));
            request.topic_data[0].name = String::new();
            Seen::Produce(13, request)
        };
        let seen = broker.seen.lock().unwrap().clone();
        assert2::assert!(seen.first() == Some(&wire(old_id)));
        assert2::assert!(seen.last() == Some(&wire(new_id)));
    }
}
