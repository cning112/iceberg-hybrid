# Iceberg Hybrid (Ports & Adapters) — Scala 3 + ZIO

This repository implements a **geo-distributed, high-availability Apache Iceberg deployment** using hexagonal (ports &
adapters) architecture with Scala 3 and ZIO functional effects.

## Architecture

### Active Modules (Scala 3 + ZIO)

- `modules/domain` — Pure domain types using Scala 3 case classes and enums
- `modules/ports` — ZIO-based interface definitions for external systems
- `modules/application` — Service orchestration using ZIO effects
- `modules/adapters` — External system implementations with ZIO integration

### Legacy Modules (Java)

- `legacy-java/*` — Original Java implementation (hexagonal architecture)
- `legacy/modules/*` — Earlier Java modules for replica-dr design

## Quick Start

### Build and Test

```bash
# Build all modules
./gradlew build

# Run tests
./gradlew test

# Clean build artifacts
./gradlew clean
```

### Development

- Open in IntelliJ IDEA with Scala plugin
- Active development in `modules/` using Scala 3 + ZIO
- Legacy Java modules in `legacy-java/` for reference
