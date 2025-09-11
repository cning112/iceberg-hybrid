# Repository Guidelines

This repository is a multi‑module Gradle project that implements a Hexagonal (Ports & Adapters) architecture for a
hybrid on‑prem + cloud Apache Iceberg setup. The active modules are Scala 3 (ZIO) under `modules/**`, and the prior Java
21 implementation has been moved under `legacy-java/**` (including integration tests). The code is organized for clear
separation of domain, ports, application services, adapters, and integration tests.

## Project Structure & Module Organization

- `modules/domain` — Scala 3 domain types and core value objects.
- `modules/ports` — Scala 3 public interfaces (traits) ending with `Port` (e.g., `CatalogPort`).
- `modules/application` — Scala 3 orchestrators/services using ports (e.g., `WriteCoordinator`, `ReadRouter`).
- `modules/adapters` — Scala 3 in‑memory and stub adapters; classes end with `Adapter`.
- `legacy-java/**` — Prior Java 21 implementation split into `*-java` modules (includes `integration-java`).
- `legacy/**` — Historical, fuller example with Spring Boot and extra QA modules.

## Local Setup

- JDK 21 required. Prefer Gradle toolchains; otherwise set `JAVA_HOME` to a JDK 21 installation.
- Scala 3 is managed via Gradle; no separate Scala installation needed.
- IDE: IntelliJ IDEA (Scala + Gradle) or VS Code with Metals/Scala + Gradle for navigation and per‑module test runs.

## Build, Test, and Development Commands

- `./gradlew build` — Build all modules (Scala + legacy Java) and run tests.
- `./gradlew test` — Run all tests across modules.
- `./gradlew :modules:application:test` — Test only the Scala application module.
- `./gradlew :modules:domain:build` — Build only the Scala domain module.
- `./gradlew :legacy-java:integration-java:test` — Run only legacy Java integration tests.
- `./gradlew :legacy-java:integration-java:test --tests "*EndToEndTest"` — Filter legacy E2E tests.
  Notes: Toolchain targets Java 21; Scala modules use Scala 3 with ZIO and zio-test (JUnit runner).

## Coding Style & Naming Conventions

- Packages: `com.streamfirst.iceberg.hybrid.*` across all modules.
- Naming: Interfaces/traits in `ports` end with `Port`; implementations in `adapters` end with `Adapter`; domain types
  are nouns in `domain`.
- Scala (active `modules/**`): Scala 3; functional style with ZIO; 2‑space or project default indent acceptable; keep
  effects explicit with `ZIO` types. Logging via `zio-logging` utilities.
- Java (legacy `legacy-java/**`): Java 21 with records; use Lombok (`@Slf4j`, `@RequiredArgsConstructor`) where present.
  Logging via SLF4J; tests use Logback via `logback-test.xml`.

## Testing Guidelines

- Scala modules: zio‑test with JUnit runner (dependencies in root `build.gradle`). Prefer `*Spec.scala` naming; one
  behavior per spec.
- Legacy Java modules: JUnit 5 (Jupiter) + AssertJ. Naming `*Test.java`.
- Integration tests: located under `legacy-java/integration-java` for now; use Gradle targets shown above.
- Run: repo‑wide with `./gradlew test`, or module‑scoped as listed in the commands section.

## Commit & Pull Request Guidelines

- Commits follow Conventional Commits (e.g., `feat(scope): …`, `refactor: …`, `docs: …`).
- PRs: Provide a clear description, link issues, include screenshots/logs if relevant, and note test coverage or new
  modules touched.
- Keep changes scoped to one feature/fix; update docs when structure or behavior changes.

PR checklist

- [ ] Conventional commit messages
- [ ] Tests updated/added and passing (`./gradlew test`)
- [ ] Docs updated (README/AGENTS) if behavior or structure changed
- [ ] No secrets committed; respects module boundaries

## Security & Configuration Tips

- Do not commit secrets or cloud credentials; use environment/config where needed.
- Keep adapters thin; prefer ports for boundaries. Avoid cross‑module leaks (no adapter → adapter coupling).

## Agent‑Specific Instructions

- Respect module boundaries and naming above; keep patches minimal and focused.
- Prefer changes in new Scala modules under `modules/**`. Avoid cross‑module leaks (no adapter → adapter coupling).
- When changes affect end‑to‑end behavior, update or mirror tests under `legacy-java/integration-java` until Scala
  integration coverage is added.
