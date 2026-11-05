# Contributing

Use JDK 17 and Maven 3.9. Start with [the test guide](docs/testing.md), then run
`mvn -s tools/maven-central-settings.xml -gs tools/maven-central-settings.xml clean verify`.
Keep changes focused and include executable evidence for the behavior you change.

## Choose the right test

- Pure state, conversion, ownership and validation contracts belong in offline tests.
- Reuse Flink SQL journeys with parameter combinations for Connector integration.
  Assert independent typed readback, cleanup and bounded recovery; a job completing
  or a configuration being accepted is not enough.
- Emulator protocol tests must independently verify service/SDK contracts. Do not
  change Emulator semantics solely to make a Connector test pass.
- CDC, DLMV, physical split planning, authentication, production quotas and distributed
  failure behavior require an approved real-service project. Run the opt-in
  `maxcompute-integration` profile described in the test guide.

Reported coverage uses a clean build and the `coverage-check` profile: 80% production
line coverage without package exclusions. Archive image identity, build logs, JaCoCo,
SQL parameters and `target/readiness/report.md`. Never count skipped suites as passed.

## Prepare a change for review

Explain the user-visible trigger, resulting behavior, test evidence and remaining
limitations. State changes to options, public APIs, serializers, checkpoint state or
delivery semantics explicitly, including migration implications.

Keep Flink dependencies `provided`. Runtime dependencies must resolve from public
repositories. Update `licenses/third-party.json` and legal files when the bundled
closure changes; run `python3 tools/check-public.py`. Pending redistribution material
must remain visible and must fail `--release`.

Do not commit credentials, environment files, customer data, logs, build outputs or
fixed shared test tables. Use disposable UUID fixtures, cancel unfinished jobs and
assert cleanup. Follow existing Java style and retain upstream license attribution.
Use the repository's review workflow for code and documentation changes.
