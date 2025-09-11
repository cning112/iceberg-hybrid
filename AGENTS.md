# Repository Guidelines

This repository is a Java 21, multi‑module Gradle project that implements a Hexagonal (Ports & Adapters) architecture for a hybrid on‑prem + cloud Apache Iceberg setup. The code is organized for clear separation of domain, ports, application services, adapters, and integration tests.

## Project Structure & Module Organization
- `modules/domain` — Immutable domain types (records) and core value objects.
- `modules/ports` — Public interfaces (ports) ending with `Port` (e.g., `CatalogPort`).
- `modules/application` — Orchestrators/services using ports (e.g., `WriteCoordinator`, `ReadRouter`).
- `modules/adapters` — In‑memory and stub adapters; classes end with `Adapter`.
- `modules/integration` — End‑to‑end tests and fixtures.
- `legacy/**` — Historical, fuller example with Spring Boot and extra QA modules.

## Local Setup
- JDK 21 required. Prefer Gradle toolchains; otherwise set `JAVA_HOME` to a JDK 21 installation.
- Use IntelliJ IDEA or VS Code Java for navigation and per‑module test runs.

## Build, Test, and Development Commands
- `./gradlew build` — Compile all modules and run tests.
- `./gradlew test` — Run all tests across modules.
- `./gradlew :modules:application:test` — Test only the application module.
- `./gradlew :modules:domain:build` — Build the domain module quickly.
- `./gradlew :modules:integration:test` — Run only integration tests.
- `./gradlew :modules:integration:test --tests "*EndToEndTest"` — Filter to E2E patterns.
Notes: Toolchain targets Java 21.

## Coding Style & Naming Conventions
- Language: Java 21 with records; use Lombok (`@Slf4j`, `@RequiredArgsConstructor`) where present.
- Indentation: 4 spaces; brace on same line; keep methods focused and side‑effect aware.
- Packages: `com.streamfirst.iceberg.hybrid.*`.
- Naming: Interfaces in `ports` end with `Port`; implementations in `adapters` end with `Adapter`; domain types are nouns in `domain`.
- Logging: SLF4J; tests use Logback via `logback-test.xml`.

## Testing Guidelines
- Frameworks: JUnit 5 (Jupiter) + AssertJ.
- Focus: Currently integration‑first in `modules/integration`. Add module‑scoped unit tests near the code as features evolve.
- Naming: `*Test.java`; each test class covers one behavior.
- Run: repo‑wide with `./gradlew test` or module‑scoped as above.

## Commit & Pull Request Guidelines
- Commits follow Conventional Commits (e.g., `feat(scope): …`, `refactor: …`, `docs: …`).
- PRs: Provide a clear description, link issues, include screenshots/logs if relevant, and note test coverage or new modules touched.
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
- Respect module boundaries and naming above; keep patches minimal and focused. Add/adjust integration tests when behavior spans modules.
