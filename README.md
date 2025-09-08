# Iceberg Hybrid (Ports & Adapters) — Java 21

This repository is a production-grade skeleton implementing a Hexagonal (Ports & Adapters) layout for a hybrid on‑prem + cloud Apache Iceberg deployment.

## Modules

- `legacy/modules/domain` — Pure domain records/entities.
- `legacy/modules/ports` — Ports (interfaces) for Catalog, ObjectStore, Inventory, Replication, Leases, Consistency, EventBus, Metrics.
- `legacy/modules/app` — Application services (ReplicationPlanner, StateReconciler, GCCoordinator, ReadRouter).
- `legacy/modules/adapters/*` — Thin adapters (stubs) for Nessie/BigLake (catalog), S3/MinIO (storage), S3 Inventory (inventory), Redis (leases/tokens), Kafka (events).
- `legacy/modules/boot` — Spring Boot wiring (onprem/cloud profiles).
- `legacy/qa/e2e` — E2E tests (in-memory happy path + optional Testcontainers scaffolding).
- `legacy/qa/perf-jmh` — JMH microbenchmarks (inventory lookup hot path).

## Quick start

```bash
./gradlew test
```

To run E2E Testcontainers (optional), enable the `containers` tag:

```bash
./gradlew :legacy:qa:e2e:test --tests "*Containers*" -Dgroups=containers
```

You can also open this project in IntelliJ IDEA and run the tests from there.
