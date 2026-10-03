# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.4.3] — 2026-10-03

### Fixed

- Send broker-specific replication throttle settings to each named broker, with topic settings grouped separately. Attempt remaining resources when clearing throttles after a broker rejects its request.

## [0.4.2] — 2026-10-03

### Fixed

- Group leader and follower throttle settings under one broker resource when applying or clearing a rebalance. This prevents duplicate-resource rejection by Kafka and Krabka Broker 0.7.0.

## [0.3.8] — 2026-06-23


### <!-- 1 -->🐛 Bug Fixes


- Echo request Content-Type on Connect responses (build_connect) ([#645](https://github.com/robot-head/crabka/pull/645))


### <!-- 10 -->💼 Other


- Remove protoc in favor of prost-native codegen ([#629](https://github.com/robot-head/crabka/pull/629))

## [0.3.7] — 2026-06-17


### <!-- 7 -->⚙️ Miscellaneous Tasks


- De-hardcode release versions + sign/attest published Helm charts ([#530](https://github.com/robot-head/crabka/pull/530))

## [0.3.6] — 2026-06-13

## [0.3.5] — 2026-06-12


### <!-- 7 -->⚙️ Miscellaneous Tasks


- Update Cargo.lock dependencies
