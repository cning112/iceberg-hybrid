# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a **hybrid Apache Iceberg deployment** implementing Hexagonal (Ports & Adapters) architecture in Java 21. The system maintains a primary, writable Iceberg database on-premise while replicating it to a read-only mirror in the cloud (AWS/GCP). The on-premise catalog serves as the **Single Source of Truth (SoT)**, ensuring uninterrupted operations even during cloud outages.

## Build and Test Commands

### Standard Operations
```bash
# Build all modules
./gradlew build

# Run all tests
./gradlew test

# Run end-to-end tests with Testcontainers (optional)
./gradlew :qa:e2e:test --tests "*Containers*" -Dgroups=containers
```

### Module Structure
- **modules/domain** — Pure domain records/entities (immutable Java records)
- **modules/ports** — Port interfaces for external systems (Catalog, ObjectStore, Inventory, etc.)
- **modules/app** — Application services implementing core business logic
- **modules/adapters/** — Concrete implementations for specific technologies (Nessie, S3, Redis, Kafka)
- **modules/boot** — Spring Boot configuration and wiring
- **qa/e2e** — End-to-end tests with optional Testcontainers
- **qa/perf-jmh** — JMH performance benchmarks

## Architecture Patterns

### Hexagonal Architecture (Ports & Adapters)
The codebase strictly follows hexagonal architecture:
- **Domain layer** contains pure business objects as Java records
- **Ports** define interfaces for all external interactions
- **Application services** orchestrate domain objects and ports
- **Adapters** provide concrete implementations for specific technologies

### Core Components
- **ReplicationPlanner** (`modules/app/src/main/java/com/yourorg/iceberg/hybrid/app/ReplicationPlanner.java:26`) — Calculates snapshot differences and creates replication plans
- **StateReconciler** (`modules/app/src/main/java/com/yourorg/iceberg/hybrid/app/StateReconciler.java:18`) — Verifies replicated files and promotes cloud catalog state
- **GCCoordinator** (`modules/app/src/main/java/com/yourorg/iceberg/hybrid/app/GCCoordinator.java:26`) — Coordinates safe garbage collection across environments
- **ReadRouter** (`modules/app/src/main/java/com/yourorg/iceberg/hybrid/app/ReadRouter.java:18`) — Routes read queries between on-premise and cloud based on consistency requirements

### Key Processes
1. **Replication Flow**: On-prem writes → ReplicationPlanner calculates diff → Copy new objects → Write `_inprogress` marker → Verify → Promote to `_ready` → Update cloud catalog
2. **Two-Phase Markers**: Uses `_inprogress/` and `_ready/` sentinel files to ensure atomic visibility of replicated snapshots
3. **Coordinated GC**: Implements safety windows and consistency tokens to prevent deletion of objects still being queried

## Development Guidelines

### Design First Approach
All development must align with the comprehensive design documented in `DESIGN.md`. This Chinese-language document contains the canonical architecture, performance optimization strategies, and operational procedures.

### Code Style
- Use Java 21 features and language constructs
- Implement domain objects as immutable `record` classes where possible
- Follow existing patterns for dependency injection and port implementations
- All new code requires JUnit 5 tests with AssertJ assertions

### Testing Strategy
- Unit tests for all application services and domain logic
- Integration tests for adapter implementations
- E2E tests in `qa/e2e` module for complete workflow validation
- Performance tests in `qa/perf-jmh` for critical paths like inventory lookup

### Adapter Implementation
When creating new adapters, follow the naming convention: `*Stub` suffix (e.g., `CatalogNessieStub`, `StorageS3Stub`) and implement the corresponding port interface from `modules/ports`.

## Key Technologies and Dependencies
- **Java 21** with Gradle build system
- **Apache Iceberg** for table format
- **Spring Boot** for application wiring (modules/boot)
- **JUnit 5 + AssertJ** for testing
- **Testcontainers** for integration testing (optional)
- **JMH** for microbenchmarks

## Performance Considerations
The system implements several optimization strategies detailed in `DESIGN.md`:
- Snapshot diffing to minimize object copying
- Inventory indexing to avoid expensive LIST operations
- Parallel replication with rate limiting
- Fast-forward replication for large version gaps
- Adaptive backpressure mechanisms