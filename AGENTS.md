# Repository Guidelines

This repository is a multi‑module Gradle project that implements a Hexagonal (Ports & Adapters)
architecture for a
hybrid on‑prem + cloud Apache Iceberg setup. The active codebase is Scala 3 (ZIO) under
`modules/**`, with growing unit
and integration test coverage. The prior Java 21 implementation has been moved under
`legacy-java/**` for reference.

## Project Structure & Module Organization

- `modules/domain` — Scala 3 domain types and core value objects (+ unit tests).
- `modules/ports` — Scala 3 public interfaces (traits) ending with `Port` (e.g., `CatalogPort`) (+
  unit tests).
- `modules/application` — Scala 3 orchestrators/services using ports (e.g., `WriteCoordinator`,
  `ReadRouter`) (+ unit tests).
- `modules/adapters` — Scala 3 in‑memory and stub adapters; classes end with `Adapter` (+ unit
  tests).
- `modules/integration` — Scala 3 end‑to‑end tests and fixtures.
- `legacy-java/**` — Prior Java 21 implementation split into `*-java` modules (includes
  `integration-java`).
- `legacy/**` — Historical, fuller example with Spring Boot and extra QA modules.

## Local Setup

- JDK 21 required. Prefer Gradle toolchains; otherwise set `JAVA_HOME` to a JDK 21 installation.
- Scala 3 is managed via Gradle; no separate Scala installation needed.
- IDE: IntelliJ IDEA (Scala + Gradle) or VS Code with Metals/Scala + Gradle for navigation and
  per‑module test runs.

## Build, Test, and Development Commands

- `./gradlew build` — Build all modules (Scala + legacy Java) and run tests.
- `./gradlew test` — Run all tests across modules.
- `./gradlew :modules:domain:test` — Run domain unit tests.
- `./gradlew :modules:ports:test` — Run ports unit tests.
- `./gradlew :modules:application:test` — Run application unit tests.
- `./gradlew :modules:adapters:test` — Run adapters unit tests.
- `./gradlew :modules:integration:test` — Run Scala end‑to‑end tests.
- `./gradlew :modules:integration:test --tests "*E2ESpec"` — Filter Scala E2E patterns.
- `./gradlew :legacy-java:integration-java:test` — Run only legacy Java integration tests.
- `./gradlew :legacy-java:integration-java:test --tests "*EndToEndTest"` — Filter legacy E2E tests.
  Notes: Toolchain targets Java 21; Scala modules use Scala 3 with ZIO and zio-test (JUnit runner).

## Coding Style & Naming Conventions

- Packages: `com.streamfirst.iceberg.hybrid.*` across all modules.
- Naming: Interfaces/traits in `ports` end with `Port`; implementations in `adapters` end with
  `Adapter`; domain types
  are nouns in `domain`.
- Scala (active `modules/**`): Scala 3; functional style with ZIO; keep effects explicit with `ZIO`
  types; prefer
  `final case class`/`enum` for domain. Logging via `zio-logging` utilities. Test classes use
  `*Spec.scala`.
- Java (legacy `legacy-java/**`): Java 21 with records; use Lombok (`@Slf4j`,
  `@RequiredArgsConstructor`) where present.
  Logging via SLF4J; tests use Logback via `logback-test.xml`.

## Testing Guidelines

- Frameworks (Scala): zio‑test with JUnit runner (dependencies in root `build.gradle`). Prefer
  `*Spec.scala`; one
  behavior per spec. Example unit specs: `RegionSpec`, `SyncEventSpec`; example E2E specs:
  `SimpleE2ESpec`,
  `WriteSyncReadWorkflowE2ESpec`, `GeoDistributedSystemE2ESpec`.
- Frameworks (Java, legacy): JUnit 5 (Jupiter) + AssertJ. Naming `*Test.java`.
- Integration tests: primary Scala E2E specs under `modules/integration`; legacy Java E2E under
  `legacy-java/integration-java` remain for comparison.
- Run: repo‑wide with `./gradlew test`, or module‑scoped as listed above. Use `--tests` with spec
  class patterns
  (e.g., `*E2ESpec`).

## Commit & Pull Request Guidelines

- Commits follow Conventional Commits (e.g., `feat(scope): …`, `refactor: …`, `docs: …`).
- PRs: Provide a clear description, link issues, include screenshots/logs if relevant, and note test
  coverage or new
  modules touched.
- Keep changes scoped to one feature/fix; update docs when structure or behavior changes.

PR checklist

- [ ] Conventional commit messages
- [ ] Tests updated/added and passing (`./gradlew test`)
- [ ] Docs updated (README/AGENTS) if behavior or structure changed
- [ ] No secrets committed; respects module boundaries

## Security & Configuration Tips

- Do not commit secrets or cloud credentials; use environment/config where needed.
- Keep adapters thin; prefer ports for boundaries. Avoid cross‑module leaks (no adapter → adapter
  coupling).

## Agent‑Specific Instructions

- Respect module boundaries and naming above; keep patches minimal and focused.
- Prefer changes in Scala modules under `modules/**`. Avoid cross‑module leaks (no adapter → adapter
  coupling).
- For end‑to‑end behavior, prefer adding/updating Scala integration specs under
  `modules/integration`. Mirror or update
  legacy Java E2E only when necessary for parity.
