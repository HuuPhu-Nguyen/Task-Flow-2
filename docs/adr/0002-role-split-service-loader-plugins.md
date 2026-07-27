# 0002: Use Role-Split ServiceLoader Plugins For Task Domains

Status: Accepted

Date: 2026-07-04

## Context

TaskFlow is intended to be a framework where new job domains can be added
without changing scheduler, transport, GUI, or executor-engine internals. The
original concrete job implementations made that boundary harder to explain and
made runtime classpaths carry dependencies that only one role needed.

Task behavior has distinct role ownership:

- coordinator-side validation, task splitting, and aggregation;
- executor-role task execution;
- requester-role local payload creation and final-result handling;
- shared model types used by more than one role.

## Decision

Task domains use role-split Maven modules under `plugins/<domain>` and are
discovered through Java `ServiceLoader` providers:

- `model` for shared payload/result/type classes;
- `server` for `server.job.TaskPlugin`;
- `client` for requester-role `client.ClientJobPlugin` implementations;
- `peer` for executor-role `peer.engine.PeerProcessorPlugin` implementations.

The `client` and `peer` module/package names are retained compatibility names;
architecturally, both run in participant nodes rather than sharing coordinator
authority.

Framework modules should not import concrete plugin implementation classes.
Runtime packages include only the role artifacts needed by that package.

## Consequences

- New task types should be added by creating plugin modules and service files,
  not by editing scheduler or transport code.
- Server plugins are the authority for accepting or rejecting submissions.
- Requester/client plugins own local file reading, payload construction, and successful
  final-result handling.
- Executor/peer plugins own processor dependencies, including heavy media/native
  dependencies.
- Role-specific dependency-tree checks are required when plugin wiring or
  package profiles change.
- Every task type binds the same reusable `PluginContractTest`; family bindings
  provide only domain samples and concrete role providers. Native codec and
  external-service behavior remains in focused role tests.

## Evidence

- `docs/PLUGIN_AUTHORING.md`
- `plugins/example`
- `plugins/conversion`
- `plugins/text`
- `taskflow-spi/src/test/java/plugin/PluginContractTest.java`
- `plugins/example/harness/src/test/java/example/harness/ExamplePluginContractHarnessTest.java`
- `plugins/text/server/src/test/java/server/plugins/text/TextAnalysisPluginContractTest.java`
- `plugins/conversion/server/src/test/java/server/plugins/conversion/ImageConversionPluginContractTest.java`
- `plugins/conversion/server/src/test/java/server/plugins/conversion/VideoTranscodingPluginContractTest.java`
- `taskflow-spi/src/main/java/server/job/TaskPlugin.java`
- `taskflow-spi/src/main/java/client/ClientJobPlugin.java`
- `taskflow-spi/src/main/java/peer/engine/PeerProcessorPlugin.java`

## Related Documents

- [Plugin Authoring](../PLUGIN_AUTHORING.md)
- [Participant Requester And Result Lifecycle](../PEER_LIFECYCLE.md)
- [Release Packaging](../RELEASE_PACKAGING.md)
- [Payload Storage](../PAYLOAD_STORAGE.md)
