# 0002: Use Role-Split ServiceLoader Plugins For Task Domains

Status: Accepted

Date: 2026-07-04

## Context

TaskFlow is intended to be a framework where new job domains can be added
without changing scheduler, transport, GUI, or peer-engine internals. The
original concrete job implementations made that boundary harder to explain and
made runtime classpaths carry dependencies that only one role needed.

Task behavior has distinct role ownership:

- coordinator-side validation, task splitting, and aggregation;
- peer-side task execution;
- submitter-side local payload creation and final-result handling;
- shared model types used by more than one role.

## Decision

Task domains use role-split Maven modules under `plugins/<domain>` and are
discovered through Java `ServiceLoader` providers:

- `model` for shared payload/result/type classes;
- `server` for `server.job.TaskPlugin`;
- `client` for `client.ClientJobPlugin`;
- `peer` for `peer.engine.PeerProcessorPlugin`.

Framework modules should not import concrete plugin implementation classes.
Runtime packages include only the role artifacts needed by that package.

## Consequences

- New task types should be added by creating plugin modules and service files,
  not by editing scheduler or transport code.
- Server plugins are the authority for accepting or rejecting submissions.
- Client plugins own local file reading, payload construction, and successful
  final-result handling.
- Peer plugins own processor dependencies, including heavy media/native
  dependencies.
- Role-specific dependency-tree checks are required when plugin wiring or
  package profiles change.
- Cross-role plugin behavior should be covered by harness tests similar to
  `plugins/example/harness`.

## Evidence

- `docs/PLUGIN_AUTHORING.md`
- `plugins/example`
- `plugins/conversion`
- `plugins/text`
- `taskflow-spi/src/main/java/server/job/TaskPlugin.java`
- `taskflow-spi/src/main/java/client/ClientJobPlugin.java`
- `taskflow-spi/src/main/java/peer/engine/PeerProcessorPlugin.java`

## Related Documents

- [Plugin Authoring](../PLUGIN_AUTHORING.md)
- [Peer Submitter And Result Lifecycle](../PEER_LIFECYCLE.md)
- [Release Packaging](../RELEASE_PACKAGING.md)
- [Payload Storage](../PAYLOAD_STORAGE.md)
