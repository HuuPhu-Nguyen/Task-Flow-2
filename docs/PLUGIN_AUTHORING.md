# TaskFlow Plugin Authoring

This guide describes how to add a new TaskFlow job type without changing
scheduler, coordinator, participant runtime, GUI, or transport internals.

TaskFlow discovers job behavior through Java `ServiceLoader` providers on the
runtime classpath:

- `server.job.TaskPlugin` for coordinator-side validation and job construction.
- `client.ClientJobPlugin` for requester-role payload creation and final-result handling.
- `peer.engine.PeerProcessorPlugin` for executor-role task execution.

The `client` and `peer` package/module names are retained compatibility names.
Both plugin types run in participant nodes; neither owns coordinator scheduling
or authoritative result commitment.

The `plugins/example` bundle is the executable authoring template and harness.
The existing `plugins/conversion` and `plugins/text` bundles are production
feature examples. A new domain should follow the same role split unless it has
a clear reason to omit a role.

## Module Layout

Create a domain folder under `plugins/<domain>` with a parent POM and role
modules:

```text
plugins/<domain>/
  pom.xml
  model/
  server/
  client/
  peer/
```

Use the modules this way:

- `model`: Shared task type constants plus payload, task-result, and final-result records used by more
  than one role. Keep it free of runtime-heavy dependencies.
- `server`: Coordinator-side `TaskPlugin`, `EmbarrassinglyParallelJob`,
  `TaskUnit`, and submission validation.
- `client`: Requester-role `ClientJobPlugin` implementations that turn local files into
  payload objects and handle final results.
- `peer`: Executor-role `PeerProcessorPlugin` implementations and `TaskProcessor`
  classes that execute coordinator-assigned tasks. Heavy processor-only
  dependencies belong here; `peer` is the retained module name.

Add the domain parent POM to the root reactor, then add its role modules to the
domain parent. Add artifacts to root dependency management if they need to be
referenced by runtime modules.

## Executable Example Harness

Use `plugins/example` as the first template when adding a new task type. It is a
small word-count plugin with the same role split expected from production
plugins:

- `model` defines `ExampleTaskTypes`, a submitted payload record, a per-task
  result record, and a semantic final summary record.
- `server` validates submissions, creates deterministic task IDs, emits
  `TaskAssignMessage` instances, records task results, and overrides
  `aggregateResultPayload()` with a semantic final summary.
- `client` reads local `.txt` files through `ClientJobPlugin.buildPayloads(...)`
  and writes a final report through `ClientJobPlugin.handleResult(...)`.
- `peer` registers an executor-role `PeerProcessorPlugin` and executes assigned word-count
  tasks.
- `harness` runs a cross-role contract test that discovers the three service
  providers, builds payloads, validates and splits a job, processes assignments,
  aggregates the semantic result, handles the final result, and verifies the
  example plugin is not wired into core or runtime POMs.

Run the executable template with:

```powershell
.\mvnw.cmd -pl plugins/example/model,plugins/example/server,plugins/example/client,plugins/example/peer,plugins/example/harness -am test
```

The example plugin is deliberately not added to `taskflow-coordinator`,
`taskflow-peer`, or `taskflow-gui` runtime dependencies. Treat it as a copyable
test harness and shape reference, not as a supported user-facing job type.

## Task Type Names

Define task type constants in the model module:

```java
public final class ExampleTaskTypes {
    public static final String EXAMPLE_ANALYSIS = "EXAMPLE_ANALYSIS";

    private ExampleTaskTypes() {
    }
}
```

Use the same constant from the server, client, and peer modules. TaskFlow
normalizes task types by trimming and uppercasing them in loader paths, but the
constant should still be stable, uppercase, and descriptive. Do not reuse an
existing task type; duplicate providers for the same type fail at runtime.

## Server Plugin

The server module owns coordinator-side validation and job construction.

Implement `server.job.TaskPlugin`:

- `taskType()` returns the shared task type constant.
- `retrySafety()` mirrors the value declared by the paired executor plugin so
  the coordinator can validate retry policy before accepting a job.
- `resourceProfile()` declares the fixed scalar capacity-unit cost and optional
  diagnostic memory/disk estimates. It must exactly match the paired executor
  declaration.
- `validateSubmission(JobSubmitMessage)` rejects bad parameters, missing
  payloads, malformed payload objects, unsupported file extensions or formats,
  and invalid encoded data before tasks are created.
- `createJob(JobSubmitMessage, String requesterId)` returns a job that can split
  the submission into task units and aggregate typed task results into a
  semantic final result payload.

Register the provider in:

```text
plugins/<domain>/server/src/main/resources/META-INF/services/server.job.TaskPlugin
```

The file contains one implementation class per line, for example:

```text
server.plugins.example.ExampleTaskPlugin
```

Server validation is the authority for accepting a submission. Client-side
validation improves local UX, but the coordinator must still reject invalid
requests because RabbitMQ submitters can send protocol messages independently
of the client-side UI or plugin validation path.

Jobs should extend `EmbarrassinglyParallelJob<T, R>` when the work can be split
into independent tasks. A job should:

- Build deterministic task IDs while initializing tasks.
- Convert raw payloads and results through typed model objects.
- Accept results only through `recordResult`; the base class already rejects
  stale results from executor participants that no longer own the task.
- Treat final aggregation as a deterministic replay function of committed task
  results. Startup may reconstruct a `FINALIZING` job and call aggregation
  again with results restored in canonical task order; do not depend on result
  arrival order, wall-clock time, randomness, process-local state, or an
  external side effect that cannot be repeated safely.
- Override `aggregateResultPayload()` when the final job result should be a
  report, summary, ranking, reduction, preview model, or another semantic
  object instead of the compatibility `List<Object>` returned by
  `aggregateAndSendResult()`.
- Include the same task type and parameter in `TaskAssignMessage` that the
  executor/peer processor expects.

## Client Plugin

The client module owns local input and output behavior for command-line
submitters and the JavaFX GUI.

This is the shared local submit/result contract. The JavaFX GUI and the
RabbitMQ command-line submitter both use `ClientJobPlugin` to build local
payloads and handle successful final results; transport and UI code decide
where results are shown or written and how handling failures are reported.

Implement `client.ClientJobPlugin`:

- `taskType()` returns the shared task type constant.
- `displayName()` is the human-readable GUI label.
- `supportedInputExtensions()` controls file chooser filtering and CLI
  validation expectations.
- `parameterOptions()` and `defaultParameter()` define the task option set.
- `buildPayloads(List<Path>, String)` reads local inputs and returns payload
  objects suitable for JSON serialization.
- `handleResult(JobResultMessage, Path)` receives the complete final result
  message and can save files, write reports, print summaries, render previews,
  or apply plugin-specific failure policy. The default implementation calls
  `saveResults(result.getResultPayloadList(), outputDir)` for compatibility.
- `saveResults(List<Object>, Path)` remains the compatibility file-save method
  for plugins whose final payload is still an ordered result list.

Register the provider in:

```text
plugins/<domain>/client/src/main/resources/META-INF/services/client.ClientJobPlugin
```

Use `PayloadLimits` when reading local inputs and writing results:

- `maxTasksPerJob()` for input count.
- `maxInputBytes()` for each local input file.
- `maxJobPayloadBytes()` for aggregate inline data or reference metadata size.
- `maxResultBytes()` for decoded result payloads written to disk.
- `maxInlinePayloadBytes()` as the exclusive raw-byte ceiling for a
  `base64Data` file body.

For large binary payloads, use the shared `ObjectReference` and
`ObjectStoreProvider` boundaries. Generate keys with `TaskFlowObjectKeys`; do
not serialize filesystem paths, arbitrary URLs, credentials, or vendor SDK
types. A requester uploads inputs and an executor downloads by key using its
own provider instance. See `docs/PAYLOAD_STORAGE.md` for the current input
ownership, limit, compatibility, cleanup, and integrity boundaries.

Use `SafeFileNames.safeOutputPath(...)` or an equivalent safe path strategy when
writing files from result-provided names. Do not trust result filenames to stay
inside the selected output directory, and do not silently overwrite duplicate
result names unless the plugin's behavior explicitly requires it.

## Executor Plugin (`peer` Compatibility Module)

The `peer` module owns executor-role task execution.

Implement `peer.engine.PeerProcessorPlugin`:

- `taskType()` returns the shared task type constant.
- `retrySafety()` declares the processor's retry behavior.
- `resourceProfile()` exactly mirrors the paired server plugin's immutable
  scheduling profile.
- `createProcessor()` returns a `TaskProcessor<?>` for that type.

Register the provider in:

```text
plugins/<domain>/peer/src/main/resources/META-INF/services/peer.engine.PeerProcessorPlugin
```

The processor receives a `TaskAssignMessage` and returns a JSON-serializable
result object. Convert `task.getPayload()` into the typed model object used by
the server job. Throw an exception when the task cannot be processed; the
participant's executor engine converts failures into failed `TASK_RESULT`
messages.

Keep native, media, ML, or other heavy execution dependencies in the `peer`
module when requester-only participants do not need them.

## Retry Safety Contract

The paired server and executor providers for every task type must return the
same non-null `RetrySafety` value:

| Declaration | Required processor behavior | Coordinator behavior |
|---|---|---|
| `PURE` | No plugin-owned durable effect occurs outside the returned result. Framework-owned result staging is part of producing that result. | Normal task retry policy is allowed. |
| `IDEMPOTENT` | Repeating the operation has the same externally visible effect as performing it once. | Normal task retry policy is allowed. |
| `REQUIRES_IDEMPOTENCY_KEY` | Every external effect uses the documented TaskFlow execution identity as its external idempotency key. | Normal task retry policy is allowed; the declaration is trusted, not inferred. |
| `UNSAFE_TO_RETRY` | An external effect cannot safely be repeated. | A new submission is rejected before job/task creation when `maxTaskRetries > 0`. The current configuration requires a positive retry value, so these plugins cannot currently accept new jobs. |

`TaskProcessor.process(TaskAssignMessage)` receives the execution context needed
by `REQUIRES_IDEMPOTENCY_KEY` processors:

- `taskId` is the logical task identity and remains stable across coordinator
  retry generations. Use it when an external operation must happen once per
  logical TaskFlow task.
- `assignmentId` identifies one assignment generation and remains stable across
  redelivery of that exact assignment. It changes when the coordinator creates
  a retry generation, so it is sufficient only when a new effect per generation
  is intentional and safe.

Plugin-specific documentation must name the external request/header/record
field that receives the chosen key and explain why its scope is correct.
TaskFlow checks that a declaration exists and contract tests must check that the
server and executor values match, but the coordinator cannot prove an external
system honored the key. Generation fencing prevents an obsolete result from
committing; it cannot undo or make arbitrary external side effects exactly once.

The example, text, image-conversion, and video-transcoding processors currently
declare `PURE`: their plugin-owned work produces the returned result and does
not commit a separate business effect to an external system.

## Runtime Classpaths

Adding a plugin is not enough; the right role artifact must be present on the
right runtime classpath.

- `taskflow-coordinator` should depend on the plugin's `server` artifact at
  runtime. It should not need client or peer artifacts.
- `taskflow-peer` `combined-runtime` should include both `client` and `peer`
  artifacts when a command-line participant enables requester and executor roles.
- `taskflow-peer` `submitter-runtime` is the requester-only compatibility profile
  and should include only `client` artifacts.
- `taskflow-peer` `executor-runtime` enables only the executor role and should
  include only `peer` artifacts.
- `taskflow-gui` follows the same `combined-runtime`, `submitter-runtime`, and
  `executor-runtime` split as `taskflow-peer`.

The coordinator and command-line participant shaded JARs already use Maven Shade's
`ServicesResourceTransformer`, so service files from plugin artifacts are
merged into packaged runtime JARs.

Release package names and smoke checks are documented in
`docs/RELEASE_PACKAGING.md`. When a plugin adds heavy runtime dependencies,
keep those dependencies in the narrowest role artifact and verify the submitter
packages still omit executor-only dependencies.

## Protocol Compatibility

Plugins should use TaskFlow SPI message classes instead of building raw protocol
JSON. New general messages emit `protocolVersion: 2`; framework-owned capacity
`PONG` messages emit inner version 3. The coordinator adds its framework-owned
assignment attempt, UUID, and lease deadline to server-plugin task templates
before publication, and the shared participant execution engine echoes the
attempt and UUID in `TASK_RESULT`. Version 0/1 task assignments and results are
rejected; semantically unchanged message types retain the legacy compatibility
documented in `docs/PROTOCOL_COMPATIBILITY.md`.

Keep plugin-owned payload and result object changes compatible within the
plugin contract. If a new task type needs a framework-level message field
outside plugin-owned payloads, update `docs/PROTOCOL_COMPATIBILITY.md` and the
shared protocol tests with the framework change.

## Focused Tests

Add tests in the plugin role modules before relying on the new type in a demo.
Use `plugins/example` for the minimum executable harness, then use the
conversion and text tests as richer production examples.

Server module tests should cover:

- `ServiceLoader.load(TaskPlugin.class)` discovers the expected task type.
- The discovered server declaration is non-null and has the expected
  `RetrySafety` value.
- The server resource profile has the intended fixed cost.
- Valid submissions are accepted.
- Invalid parameters are rejected.
- Empty or malformed payload lists are rejected.
- Unsupported input shape, extension, format, or encoded data is rejected.
- Job initialization, result recording, and aggregation work for typed payloads
  when the job has nontrivial aggregation behavior.

Client module tests should cover:

- `ServiceLoader.load(ClientJobPlugin.class)` discovers the expected task type.
- Payload building reads representative local inputs into the expected typed
  payloads.
- Parameter normalization accepts supported case variants and rejects unknown
  options.
- Unsupported input extensions are rejected.
- `PayloadLimits` failures are surfaced for input count, input bytes, and total
  job payload bytes.
- Object-backed payload tests cover exclusive inline boundaries, upload
  failure, portable serialized references, download by key through a separate
  provider instance, and best-effort cleanup after a failed payload build.
- Legacy local-file references must be rejected. Missing provider/configuration
  must fail object-backed work without falling back to a filesystem path.
- Read object-backed input and result content through
  `PayloadIntegrityVerifier`; exact length or SHA-256 mismatch must fail before
  processor work or requester output acceptance.
- Result handling writes or presents the expected final payload, stays inside
  the selected output directory when writing files, handles duplicate names
  safely, and enforces result-size limits when the result contains file data.

Peer module tests should cover:

- `ServiceLoader.load(PeerProcessorPlugin.class)` discovers the expected task
  type.
- The discovered executor declaration is non-null and has the expected
  `RetrySafety` value.
- The executor resource profile exactly matches the server declaration.
- The processor returns the expected typed result for a representative
  `TaskAssignMessage`.
- Processor failure behavior is tested directly when invalid task payloads or
  unsupported parameters are realistic.

If a new plugin changes runtime dependencies or role classpaths, run dependency
tree checks for the affected modules in addition to focused tests.

For new domains, keep a harness test similar to
`plugins/example/harness/src/test/java/example/harness/ExamplePluginContractHarnessTest.java`.
That test should prove the new plugin can run across client, server, peer, and
result-handler contracts, and that the paired server/executor retry-safety
declarations match, without core, scheduler, transport, GUI, or peer-engine
source changes.

## Add A New Task Type Without Touching Core

Use this checklist for each new task type:

1. Add a task type constant and shared payload/result records in
   `plugins/<domain>/model`.
2. Add a server `TaskPlugin`, retry-safety and resource-profile declarations,
   job, task unit, validation helper, and
   `META-INF/services/server.job.TaskPlugin` entry.
3. Add a client `ClientJobPlugin`, payload/final-result handling, payload-limit
   checks, safe output naming, and `META-INF/services/client.ClientJobPlugin`
   entry.
4. Add a peer `PeerProcessorPlugin`, matching retry-safety and resource-profile
   declarations, processor implementation, and
   `META-INF/services/peer.engine.PeerProcessorPlugin` entry.
5. Wire the domain modules into Maven reactor and dependency management.
6. Add server artifacts to `taskflow-coordinator` runtime dependencies.
7. Add client and peer artifacts to `taskflow-peer` and `taskflow-gui` profiles
   according to `combined-runtime`, `submitter-runtime`, and `executor-runtime`.
8. Add focused server, client, peer, and harness tests for discovery,
   matching retry-safety and resource-profile declarations, validation,
   payload creation,
   final-result handling, processing, aggregation, and no-core-change wiring.
9. Run the focused tests for the new plugin modules and harness.
10. Run `git diff --check`; run broader Maven and dependency-tree gates if the
    plugin changed runtime classpaths or packaged runtime behavior.

If any step requires changing scheduler, transport, GUI, or peer engine code,
stop and check whether the new job type is missing a plugin-owned contract or
whether the framework needs an explicit new extension point.
