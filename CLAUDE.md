# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This repository implements a **geo-distributed, high-availability Apache Iceberg deployment** using hexagonal (ports & adapters) architecture. The system enables transactional data lake operations across multiple geographic locations with global consistency and local low latency.

**Core Technologies:**
- Java 21
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

**Note:** Legacy modules exist but are not actively developed or used. New development will focus on implementing the geo-distributed HA architecture.

## Architecture Overview

This project will implement a **geo-distributed, high-availability Apache Iceberg deployment** based on the `iceberg-arch-geo-distributed-ha.md` design document.

### Current State
- Legacy modules exist but are not actively used or developed
- New modules will be created following hexagonal (ports & adapters) architecture
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

### Module Structure (Future)
When implementing new modules, follow hexagonal architecture:
- **Domain** — Pure business logic, no external dependencies
- **Ports** — Interface definitions for external systems
- **Application** — Service orchestration and business workflows  
- **Adapters** — External system implementations

### Key Design Principles
- **Geographic Awareness** — All components must consider multi-region operations
- **Consistency Models** — Strong consistency for metadata, eventual consistency for data
- **Failure Resilience** — Design for regional failures and network partitions
- **Performance Optimization** — Local reads, coordinated writes

### Reference Documents
- `iceberg-arch-geo-distributed-ha.md` — Primary architectural design
- `GEMINI.md` — Detailed project intelligence and implementation guidance