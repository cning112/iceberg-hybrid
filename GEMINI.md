# GEMINI.md - Project Intelligence Report

## 1. Project Overview & Core Purpose

This project implements a **hybrid Apache Iceberg deployment model**. The central goal, as detailed in `DESIGN.md`, is to maintain a primary, writable Iceberg database on-premise while replicating it to a **read-only mirror** in a cloud environment (AWS/GCP).

This architecture ensures that on-prem write operations can continue uninterrupted even if the cloud control plane is down, establishing the on-prem catalog as the **Single Source of Truth (SoT)**. Cloud-based analytics platforms (like BigQuery, Dataproc, or Trino) can then query the replicated data with a predictable, low-latency delay.

**Core Principles:**
*   **Single Source of Truth (SoT):** All writes happen on-premise through a single REST Catalog.
*   **Commit Consistency:** The cloud mirror is updated atomically, preventing partial or inconsistent views of the data.
*   **Decoupled Operations:** Replication, garbage collection, and data consumption are designed as independent, coordinated processes.

**Key Technologies:**
*   **Language:** Java 21
*   **Build Tool:** Gradle
*   **Core Framework:** Apache Iceberg
*   **Architecture:** Hexagonal (Ports & Adapters)

---

## 2. Architecture and Data Flow

The system is split between an "On-Prem Side" and a "Cloud Side". The data flows in one direction: from on-prem to the cloud.

```
On-Prem                                     Cloud
------------------------------------        ------------------------------------
[Write Engines (Spark/Flink)]
       │
       ▼
[REST Catalog (SoT)] ◄───────► [Object Storage (MinIO/S3)]
       │                                │
       │ (New Snapshot vN)              │ (Data Files)
       │                                │
       ▼                                ▼
[Replicator Service] ───────────► [Cloud Storage (GCS/S3)]
       │ (Replication Plan)             │ (Objects + _inprogress/vN.marker)
       │                                │
       │                                ▼
       │                         [Follower/Synchronizer Service]
       │                                │ (Reads _ready/vN.marker)
       │                                ▼
       │                         [BLMS (Read-Only Catalog)]
       │                                │
       │                                ▼
       │                         [Query Engines (BigQuery)]
```

### Key Components & Code Implementation

| Design Component | Code Implementation | Purpose |
| :--- | :--- | :--- |
| **REST Catalog (SoT)** | `ports/CatalogPort` (on-prem impl) | The single source of truth for all table metadata and commits. |
| **Replicator (复制器)** | `app/ReplicationPlanner.java` | Calculates the difference between the on-prem (vN) and cloud (vK) snapshots and creates a plan to copy only the new files. This implements the **snapshot diffing** (`快照差分`) strategy. |
| **Marker Files (哨兵文件)** | `_inprogress/` & `_ready/` dirs | Sentinel files in cloud storage that signal the state of replication. The `Replicator` writes an `_inprogress` marker, and the `Follower` promotes it to `_ready` after verification. |
| **Follower/Synchronizer (同步器)** | `app/StateReconciler.java` | Triggered by the presence of a marker. The `verifyAndPromote` method validates that all files in the snapshot exist in cloud storage and then updates the read-only cloud catalog (BLMS) to make the new snapshot visible. This is the second phase of the **two-phase marker** system. |
| **GC Coordinator (垃圾回收协同)** | `app/GCCoordinator.java` | Implements the safe, coordinated garbage collection process. It uses a `DeletePlan` from the SoT and a `SafetyWindow` (grace period) to ensure data isn't deleted from cloud storage while it's still being queried. |
| **Read Router** | `app/ReadRouter.java` | An application-level component (not explicitly in the core replication design) that can direct read queries to either on-prem or the cloud based on a defined policy and data freshness (`ConsistencyToken`). |

---

## 3. Building and Running

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

To run the end-to-end tests that use Testcontainers:
```bash
./gradlew :qa:e2e:test --tests "*Containers*" -Dgroups=containers
```

---

## 4. Development Conventions

*   **Design First:** The `DESIGN.md` document is the canonical source for the system's architecture and principles. Any significant changes or new features **must** align with this design.
*   **Ports & Adapters:** The hexagonal architecture is strictly enforced.
    *   **Domain (`modules/domain`):** Contains pure, framework-agnostic business objects.
    *   **Ports (`modules/ports`):** Defines the interfaces for all external interactions (e.g., `CatalogPort`, `ObjectStorePort`).
    *   **Application (`modules/app`):** Contains the core application logic that orchestrates the domain objects and ports.
    *   **Adapters (`modules/adapters`):** Provides concrete implementations of the ports for specific technologies (e.g., S3, Redis, Nessie).
*   **Testing:** All new code should be accompanied by tests. Use JUnit 5 and AssertJ.
*   **Immutability:** Domain objects are implemented as Java `record` classes where possible to promote immutability.