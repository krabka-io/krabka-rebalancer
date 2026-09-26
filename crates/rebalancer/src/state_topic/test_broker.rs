//! In-process single-broker fixture for state-topic tests.
//!
//! It wraps `crabka_client_core::MockBroker` with the framing a typed test
//! needs. It answers `ApiVersions` with the ranges a current Kafka broker
//! advertises for Metadata, Fetch and Produce. It answers Metadata with the
//! state topic's current id and this broker as the partition-0 leader. It
//! decodes every other request at the negotiated version and hands it to the
//! test.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU16, Ordering},
};

use bytes::BytesMut;
use crabka_client_core::{Client, MockBroker};
use crabka_protocol::{
    Decode, Encode, ProtocolRequest,
    owned::{
        api_versions_request,
        api_versions_response::{ApiVersion, ApiVersionsResponse},
        fetch_request::{self, FetchRequest},
        fetch_response::FetchResponse,
        metadata_request,
        metadata_response::{
            MetadataResponse, MetadataResponseBroker, MetadataResponsePartition,
            MetadataResponseTopic,
        },
        produce_request::{self, ProduceRequest},
        produce_response::ProduceResponse,
    },
    primitives::uuid::Uuid,
};
use crabka_units::{Time, secs};

/// The broker's node id, which Metadata also names as the partition-0 leader.
pub(crate) const NODE_ID: i32 = 1;

/// Request timeout for clients of the fixture. It bounds a test that the
/// fixture leaves unanswered.
const CLIENT_TIMEOUT: Time = secs(5);

/// A data-plane request the fixture decoded, with its negotiated version.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Seen {
    Fetch(i16, FetchRequest),
    Produce(i16, ProduceRequest),
}

/// How the test answers a data-plane request.
pub(crate) type Responder = Box<dyn FnMut(&Seen) -> Answer + Send>;

/// A typed answer to a data-plane request.
pub(crate) enum Answer {
    Fetch(FetchResponse),
    Produce(ProduceResponse),
}

pub(crate) struct TestBroker {
    pub client: Arc<Client>,
    /// The state topic's id that Metadata reports. A test changes it to model
    /// a topic that was deleted and recreated.
    pub topic_id: Arc<Mutex<Uuid>>,
    /// Every data-plane request, in arrival order.
    pub seen: Arc<Mutex<Vec<Seen>>>,
    _broker: MockBroker,
}

impl TestBroker {
    pub async fn start(topic: &str, topic_id: Uuid, mut respond: Responder) -> Self {
        let port = Arc::new(AtomicU16::new(0));
        let current_id = Arc::new(Mutex::new(topic_id));
        let seen = Arc::new(Mutex::new(Vec::new()));
        let handler = {
            let port = Arc::clone(&port);
            let current_id = Arc::clone(&current_id);
            let seen = Arc::clone(&seen);
            let topic = topic.to_owned();
            move |api_key: i16, version: i16, _corr: i32, body: &[u8]| -> Option<Vec<u8>> {
                match api_key {
                    api_versions_request::API_KEY => Some(api_versions()),
                    metadata_request::API_KEY => Some(response_frame::<
                        crabka_protocol::owned::metadata_request::MetadataRequest,
                    >(
                        &metadata(
                            &topic,
                            *current_id.lock().unwrap(),
                            port.load(Ordering::SeqCst),
                        ),
                        version,
                    )),
                    fetch_request::API_KEY => {
                        let request = Seen::Fetch(version, decode_request(body, version));
                        seen.lock().unwrap().push(request.clone());
                        let Answer::Fetch(response) = respond(&request) else {
                            panic!("fetch must be answered with a fetch response");
                        };
                        Some(response_frame::<FetchRequest>(&response, version))
                    }
                    produce_request::API_KEY => {
                        let request = Seen::Produce(version, decode_request(body, version));
                        seen.lock().unwrap().push(request.clone());
                        let Answer::Produce(response) = respond(&request) else {
                            panic!("produce must be answered with a produce response");
                        };
                        Some(response_frame::<ProduceRequest>(&response, version))
                    }
                    _ => None,
                }
            }
        };
        let broker = MockBroker::start(handler).await;
        port.store(broker.addr.port(), Ordering::SeqCst);
        let client = Client::builder()
            .bootstrap(broker.addr.to_string())
            .client_id("state-topic-test-broker")
            .request_timeout(CLIENT_TIMEOUT)
            .build()
            .await
            .expect("client build does not connect");
        Self {
            client: Arc::new(client),
            topic_id: current_id,
            seen,
            _broker: broker,
        }
    }
}

/// `ApiVersions` v0 body with the ranges the Kafka 4.x broker advertises for
/// the APIs the state topic uses. Fetch tops out at v18 and Produce at v13,
/// the first versions of each that name a topic by id alone (KIP-516).
fn api_versions() -> Vec<u8> {
    let range = |api_key, min_version, max_version| ApiVersion {
        api_key,
        min_version,
        max_version,
        ..Default::default()
    };
    let response = ApiVersionsResponse {
        api_keys: vec![
            range(api_versions_request::API_KEY, 0, 4),
            range(metadata_request::API_KEY, 0, 13),
            range(fetch_request::API_KEY, 4, 18),
            range(produce_request::API_KEY, 3, 13),
        ],
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    response.encode(&mut buf, 0).expect("encode ApiVersions");
    buf.to_vec()
}

fn metadata(topic: &str, topic_id: Uuid, port: u16) -> MetadataResponse {
    MetadataResponse {
        brokers: vec![MetadataResponseBroker {
            node_id: NODE_ID,
            host: "127.0.0.1".into(),
            port: i32::from(port),
            ..Default::default()
        }],
        topics: vec![MetadataResponseTopic {
            name: Some(topic.into()),
            topic_id,
            partitions: vec![MetadataResponsePartition {
                partition_index: 0,
                leader_id: NODE_ID,
                replica_nodes: vec![NODE_ID],
                isr_nodes: vec![NODE_ID],
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// Strip request header v1/v2's `client_id` and tagged fields, then decode the
/// body at `version`.
fn decode_request<R>(body: &[u8], version: i16) -> R
where
    R: ProtocolRequest + for<'de> Decode<'de>,
{
    let client_id_len = usize::from(u16::from_be_bytes([body[0], body[1]]));
    let mut cursor = &body[2 + client_id_len..];
    if version >= R::FLEXIBLE_MIN {
        cursor = &cursor[1..];
    }
    let request = R::decode(&mut cursor, version).expect("decode request");
    assert2::assert!(cursor.is_empty());
    request
}

/// Response header v0/v1 then the body. The mock prepends the correlation id.
fn response_frame<R: ProtocolRequest>(response: &impl Encode, version: i16) -> Vec<u8> {
    let mut buf = BytesMut::new();
    if version >= R::FLEXIBLE_MIN {
        buf.extend_from_slice(&[0]);
    }
    response.encode(&mut buf, version).expect("encode response");
    buf.to_vec()
}
