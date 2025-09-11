# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This repository implements a **geo-distributed, high-availability Apache Iceberg deployment** using hexagonal (ports &
adapters) architecture. The system enables transactional data lake operations across multiple geographic locations with
global consistency and local low latency.

**Core Technologies:**

- Scala 3 (migrated from Java 21)
- ZIO functional effects library
- Gradle build system
- Apache Iceberg
- Project Nessie (transactional catalog)

## Build and Development Commands

### Core Commands

```bash
# Run all tests
./gradlew test

# Build the project
./gradlew build

# Clean build artifacts
./gradlew clean
```

**Note:** The project has been migrated from Java to Scala 3 + ZIO. Legacy Java modules exist in `legacy-java/` for
reference but are not actively developed. New development focuses on the Scala modules in `modules/` implementing the
geo-distributed HA architecture.

## Architecture Overview

This project will implement a **geo-distributed, high-availability Apache Iceberg deployment** based on the
`iceberg-arch-geo-distributed-ha.md` design document.

### Current State

- Migrated from Java to Scala 3 + ZIO functional architecture
- Active development in `modules/` with hexagonal (ports & adapters) architecture
- Legacy Java modules preserved in `legacy-java/` for reference
- Focus is on implementing the geo-distributed HA design

### Target Architecture (To Be Implemented)

The system will implement a **peer-to-peer network of geographic locations** with:

**Core Components:**

- **Global Transactional Catalog** — Single source of truth using Project Nessie
- **Regional Commit Gates** — Ensure write consistency before replication
- **Synchronization Services** — Decentralized data movement between regions
- **Data Movers** — Handle physical file replication across storage systems
- **Storage Registry** — Global registry of storage locations and policies

**Key Architectural Patterns:**

- **Hexagonal Architecture** — Clear separation between domain, ports, and adapters
- **Event-Driven Synchronization** — Asynchronous replication between regions
- **Multi-Region Consistency** — Global metadata consistency with local read optimization

## Development Guidelines

### Module Structure

Current Scala 3 + ZIO modules following hexagonal architecture:

- **`modules/domain`** — Pure domain types and business logic using Scala 3 case classes and enums
- **`modules/ports`** — ZIO-based interface definitions for external systems
- **`modules/application`** — Service orchestration and business workflows using ZIO effects
- **`modules/adapters`** — External system implementations with ZIO integration

### Key Design Principles

- **Geographic Awareness** — All components must consider multi-region operations
- **Consistency Models** — Strong consistency for metadata, eventual consistency for data
- **Failure Resilience** — Design for regional failures and network partitions
- **Performance Optimization** — Local reads, coordinated writes

### Reference Documents

- `iceberg-arch-geo-distributed-ha.md` — Primary architectural design
- `GEMINI.md` — Detailed project intelligence and implementation guidance