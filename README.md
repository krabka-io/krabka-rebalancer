# krabka-rebalancer

[![Crates.io](https://img.shields.io/crates/v/krabka-rebalancer.svg)](https://crates.io/crates/krabka-rebalancer)
[![Docs.rs](https://docs.rs/krabka-rebalancer/badge.svg)](https://docs.rs/krabka-rebalancer)
[![CI](https://github.com/krabka-io/krabka-rebalancer/actions/workflows/ci.yml/badge.svg)](https://github.com/krabka-io/krabka-rebalancer/actions/workflows/ci.yml)

Cruise-Control-equivalent partition rebalancer for Krabka clusters.

This repository owns the standalone rebalancer service and its Helm chart for the [Krabka](https://github.com/krabka-io) ecosystem.

## Install

```sh
cargo add krabka-rebalancer
```

For source builds, use either `cargo build --workspace` or `bazel build //...`.

## Usage example

Evaluate a leader-distribution goal against an in-memory cluster model:

```rust,no_run
use std::sync::Arc;
use krabka_rebalancer::capacity::BrokerCapacities;
use krabka_rebalancer::goals::{GoalContext, leader_distribution::LeaderDistribution};
use krabka_rebalancer::model::{BrokerView, ClusterState, PartitionView};
use krabka_rebalancer::optimizer;
use krabka_rebalancer::scraper::UsageStore;
use crabka_units::percent;

# fn run() -> Result<(), Box<dyn std::error::Error>> {
let state = ClusterState {
    cluster_id: Some("cluster-a".into()),
    snapshot_at_ms: 1_713_000_000_000,
    brokers: vec![
        BrokerView { id: 1, host: "b1".into(), port: 9092, rack: None },
        BrokerView { id: 2, host: "b2".into(), port: 9092, rack: None },
    ],
    partitions: vec![PartitionView {
        topic: "orders".into(),
        partition: 0,
        replicas: vec![1, 2],
        leader: 1,
        isr: vec![1, 2],
    }],
    in_flight_reassignments: Vec::new(),
};
let ctx = GoalContext {
    imbalance_threshold: percent(10),
    max_movements_per_proposal: 100,
    min_topic_leaders_per_broker: 0,
    broker_capacities: Arc::new(BrokerCapacities::default()),
    broker_usages: Arc::new(UsageStore::default()),
};
let goal = LeaderDistribution;
let out = optimizer::optimize(&state, &[&goal], &ctx)?;
println!("{} proposed movements", out.proposal.movements.len());
# Ok(())
# }
```

## Documentation

Read the API documentation at [docs.rs/krabka-rebalancer](https://docs.rs/krabka-rebalancer).

## License

Apache-2.0. See the repository `LICENSE` and `NOTICE` files for details.
