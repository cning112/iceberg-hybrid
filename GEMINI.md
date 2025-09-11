# GEMINI.md - Project Intelligence Report

## 1. Project Overview & Core Purpose

This project implements a **geo-distributed, high-availability Apache Iceberg deployment model**. The central goal is to
support transactional data lake operations (reads and writes) from multiple geographic locations simultaneously, while
ensuring low latency for local operations and maintaining global data consistency.

This architecture enables true multi-site read/write capabilities, making it suitable for global enterprises that
require a single, consistent view of their data lake across disparate cloud and on-premise environments.

**Current Development Status:**

* **Legacy Modules:** The existing modules under the `legacy-java/` directory are no longer actively maintained or used
  for
  new development. They serve as a historical reference.
* **New Modules:** New modules are being actively developed in Scala, following the design principles outlined in
  `iceberg-arch-geo-distributed-ha.md`. These new modules will gradually replace the functionality of the legacy
  modules.

**Core Principles:**

* **Single Source of Truth (for Metadata):** A globally consistent, transactional catalog (Project Nessie) acts as the
  single source of truth for all table metadata commits.
* **Geo-Distributed Writes & Reads:** Write jobs execute in their local environment for performance, and read jobs query
  a complete, localized data replica for low latency.
* **High Availability:** The architecture is designed to be resilient to the failure of any single geographic location,
  for both read and write operations.
* **Decoupled Synchronization:** Data replication and metadata localization are handled by a decentralized mesh of
  services, ensuring no single point of failure in the data plane.

**Key Technologies:**

* **Language:** Scala
* **Legacy Language:** Java 21
* **Build Tool:** Gradle
* **Core Frameworks:** Apache Iceberg, Project Nessie
* **Architecture:** Decoupled Control Plane (Catalog) and Data Plane (Synchronization)

---

## 2. Architecture and Data Flow

The system is a peer-to-peer network of locations, coordinated by a central catalog. Each location can support both
reads and writes, and is responsible for synchronizing data with all other locations.

```
                                  ┌──────────────────────────────────┐
                                  │      Global Transactional        │
                                  │ Catalog (Nessie + CockroachDB/DynamoDB) │
                                  └──────────────────┬─────────────────┘
                                                     │ (Commits to 'main' branch)
                                                     │
                         ┌───────────────────────────┴───────────────────────────┐
                         │ (Write jobs commit metadata with absolute, hetero URIs) │
                         ▼                                                       ▼
┌──────────────────────────────────┐                          ┌──────────────────────────────────┐
│        Location: US-East         │                          │        Location: EU-Central      │
│                                  │                          │                                  │
│  [Write/Query Engines]           │                          │  [Write/Query Engines]           │
│           │                      │                          │           │                      │
│           ▼                      │                          │           ▼                      │
│  ┌───────────────────┐           │                          │  ┌───────────────────┐           │
│  │   S3 Storage      │◄─────────┼──────────────────────────┼──►│   S3 Storage      │           │
│  │ (us-east-1)       │  (Sync)   │                          │  │ (eu-central-1)    │           │
│  └───────────────────┘           │                          │  └───────────────────┘           │
│           ▲                      │                          │           ▲                      │
│           │ (Reads from local)   │                          │           │ (Reads from local)   │
│           │                      │                          │           │                      │
│  ┌───────────────────┐           │                          │  ┌───────────────────┐           │
│  │  Sync Service     ├───────────┘                          └───────────┤  Sync Service     │           │
│  │  (Airflow + Rclone)│                                                │  (Airflow + Rclone)│           │
│  └───────────────────┘                                                └───────────────────┘           │
│           │                                                                     │                      │
│           └────────────► Reads 'main', Writes 'main_replica_us' ◄────────────┘                      │
│                                                                                                      │
└──────────────────────────────────┘                          └──────────────────────────────────┘
```

---

## 3. Key Components & Code Implementation

| Design Component                 | Code Implementation / Technology                                                                                            | Purpose                                                                                                                                  |
|:---------------------------------|:----------------------------------------------------------------------------------------------------------------------------|:-----------------------------------------------------------------------------------------------------------------------------------------|
| **Global Transactional Catalog** | **Project Nessie** service cluster, backed by a distributed DB like **CockroachDB** or a managed service like **DynamoDB**. | The single source of truth for all metadata transactions. Provides atomic commits and consistent branching logic.                        |
| **Synchronization Service**      | A decentralized mesh of services, likely implemented with **Apache Airflow**. One instance per location.                    | Orchestrates the two-phase synchronization process: pulling foreign data files and localizing metadata.                                  |
| **Data Mover**                   | **Rclone**, called by the Synchronization Service.                                                                          | The "muscle" that performs the physical, idempotent, and parallel copying of data files between heterogeneous storage backends.          |
| **Storage Registry**             | A shared JSON/YAML configuration file.                                                                                      | A lookup service for the Synchronization Service to resolve foreign storage path prefixes to credentials and API endpoints.              |
| **Localized Replica Branch**     | A branch in Nessie, e.g., `main_replica_us`.                                                                                | A read-only, localized version of the metadata, containing only paths to local storage. This is the entry point for local query engines. |

---

## 4. Building and Running

### Building the Project

To build all modules and run verification checks, use the Gradle wrapper:

```bash
./gradlew build
```

### Running Tests

To run all standard unit and integration tests:

```bash
./gradlew test
```

To run the end-to-end tests that use Testcontainers (e.g., testing against a local Nessie/MinIO setup):

```bash
./gradlew :qa:e2e:test --tests "*Containers*" -Dgroups=containers
```

---

## 5. Development Conventions

* **Design First:** The `iceberg-arch-geo-distributed-ha.md` document is the canonical source for the system's
  architecture and principles. Any significant changes or new features **must** align with this design.
* **Ports & Adapters:** The hexagonal architecture is strictly enforced, especially for the Synchronization Service, to
  keep orchestration logic separate from data-moving implementation.
* **Testing:** All new code should be accompanied by tests.
* **Immutability:** Domain objects are implemented as Scala `case classes` where possible to promote immutability.
