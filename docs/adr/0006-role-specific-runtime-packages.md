# 0006: Publish Role-Specific Runtime Packages

Status: Accepted

Date: 2026-07-04

## Context

TaskFlow runtime roles do not need the same dependencies. A requester-enabled
participant needs client plugins for local payload creation and result handling,
while an executor-enabled participant needs peer processor plugins and may need
heavy native dependencies. The coordinator needs server plugins but not GUI,
client, or peer processor artifacts.

A single all-in-one runtime package makes local demos convenient but hides role
boundaries and carries unnecessary dependencies into narrow deployments.

## Decision

TaskFlow publishes role-specific runtime packages:

- coordinator shaded runtime jar;
- command-line participant combined, requester (`submitter-runtime`), and
  executor shaded jars under the retained `taskflow-peer` artifact name;
- JavaFX participant classpath package with combined, requester
  (`submitter-runtime`), and executor dependency
  profiles;
- plugin bundles as role-split Maven artifacts.

Docker Compose remains the local broker-backed demo distribution, not the only
release packaging mechanism.

## Consequences

- `combined-runtime` stays convenient for local demos.
- `submitter-runtime` is the compatibility profile name for the requester role;
  it omits peer processor artifacts and heavy executor-only
  dependencies.
- `executor-runtime` omits client plugin artifacts.
- JavaFX remains classpath-based because JavaFX dependencies are
  platform-specific and should remain visible to packagers.
- Package role changes require dependency-tree checks and smoke checks for the
  affected packages.
- The example plugin remains an authoring template and harness unless it is
  deliberately promoted into runtime packages.

## Evidence

- `docs/RELEASE_PACKAGING.md`
- `pom.xml`
- `taskflow-coordinator/pom.xml`
- `taskflow-peer/pom.xml`
- `taskflow-gui/pom.xml`
- `Dockerfile`
- `docker-compose.yml`

## Related Documents

- [Release Packaging](../RELEASE_PACKAGING.md)
- [Plugin Authoring](../PLUGIN_AUTHORING.md)
- [RabbitMQ Runtime Scope Decision](../RABBITMQ_SCOPE.md)
