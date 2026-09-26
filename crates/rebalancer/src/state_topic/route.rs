//! Where the state topic's single partition lives: the topic id that Kafka's
//! id-keyed Fetch and Produce versions name it by, and the broker that leads
//! partition 0. The loader and the producer both resolve it through Metadata.

use crabka_client_core::Client;
use crabka_protocol::{owned::metadata_response::MetadataResponse, primitives::uuid::Uuid};

use crate::state_topic::error::StateTopicError;

/// The state topic's id and its partition-0 leader.
///
/// Fetch v13+ and Produce v13+ carry only the topic id on the wire, and Kafka
/// answers `UNKNOWN_TOPIC_ID` for an id it cannot resolve, including the zero
/// id. A request to any broker but the leader answers
/// `NOT_LEADER_OR_FOLLOWER`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TopicRoute {
    pub topic_id: Uuid,
    pub leader_id: i32,
}

/// Resolve `topic`'s route through Metadata. Returns `Ok(None)` when the
/// metadata response has no usable entry for the topic. The topic then exists
/// in the controller's metadata image but has not propagated to the broker on
/// the other end of this connection. Treat that case as transient and retry.
pub(crate) async fn resolve_topic_route(
    client: &Client,
    topic: &str,
) -> Result<Option<TopicRoute>, StateTopicError> {
    let resp = client.refresh_metadata().await?;
    Ok(topic_route_from_metadata(&resp, topic))
}

fn topic_route_from_metadata(resp: &MetadataResponse, topic: &str) -> Option<TopicRoute> {
    let topic = resp
        .topics
        .iter()
        .find(|t| t.name.as_deref() == Some(topic))
        .filter(|t| t.topic_id != Uuid::default())?;
    let partition = topic
        .partitions
        .iter()
        .find(|partition| partition.partition_index == 0 && partition.leader_id >= 0)?;
    Some(TopicRoute {
        topic_id: topic.topic_id,
        leader_id: partition.leader_id,
    })
}

#[cfg(test)]
mod tests {
    use crabka_protocol::owned::metadata_response::{
        MetadataResponse, MetadataResponsePartition, MetadataResponseTopic,
    };
    use crabka_units::{Time, millis};

    use super::*;

    /// Connect and request timeout for the deliberately unreachable test
    /// client.
    const CLIENT_TIMEOUT: Time = millis(50);

    fn metadata_topic(name: &str, topic_id: Uuid, leader_id: i32) -> MetadataResponseTopic {
        MetadataResponseTopic {
            name: Some(name.into()),
            topic_id,
            partitions: vec![MetadataResponsePartition {
                partition_index: 0,
                leader_id,
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    #[test]
    fn topic_route_from_metadata_requires_id_and_partition_zero_leader() {
        let resp = MetadataResponse {
            topics: vec![
                metadata_topic("other-topic", Uuid([9; 16]), 4),
                metadata_topic("state-topic", Uuid([7; 16]), 7),
                metadata_topic("zero-id", Uuid::default(), 7),
                metadata_topic("leaderless", Uuid([5; 16]), -1),
            ],
            ..Default::default()
        };

        for (topic, want) in [
            (
                "state-topic",
                Some(TopicRoute {
                    topic_id: Uuid([7; 16]),
                    leader_id: 7,
                }),
            ),
            (
                "other-topic",
                Some(TopicRoute {
                    topic_id: Uuid([9; 16]),
                    leader_id: 4,
                }),
            ),
            ("zero-id", None),
            ("leaderless", None),
            ("missing", None),
        ] {
            assert2::assert!(topic_route_from_metadata(&resp, topic) == want, "{topic}");
        }
    }

    #[tokio::test]
    async fn resolve_topic_route_propagates_metadata_send_errors() {
        let client = Client::builder()
            .bootstrap("127.0.0.1:1")
            .client_id("state-topic-route-test")
            .connect_timeout(CLIENT_TIMEOUT)
            .request_timeout(CLIENT_TIMEOUT)
            .build()
            .await
            .expect("client build does not connect");

        assert2::assert!(
            resolve_topic_route(&client, "__krabka_state")
                .await
                .is_err()
        );
    }
}
