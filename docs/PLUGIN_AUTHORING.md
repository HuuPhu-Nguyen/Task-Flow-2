# TaskFlow Plugin Authoring

This guide describes how to add a new TaskFlow job type without changing
scheduler, coordinator, GUI, peer, or transport internals.

TaskFlow discovers job behavior through Java `ServiceLoader` providers on the
runtime classpath:

- `server.job.TaskPlugin` for coordinator-side validation and job construction.
- `client.ClientJobPlugin` for client-side payload creation and result saving.
- `peer.engine.PeerProcessorPlugin` for peer-side task execution.

The existing `plugins/conversion` and `plugins/text` bundles are the reference
implementations. A new domain should follow the same role split unless it has a
clear reason to omit a role.

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

- `model`: Shared task type constants plus payload/result records used by more
  than one role. Keep it free of runtime-heavy dependencies.
- `server`: Coordinator-side `TaskPlugin`, `EmbarrassinglyParallelJob`,
  `TaskUnit`, and submission validation.
- `client`: `ClientJobPlugin` implementations that turn local files into
  payload objects and save final results to disk.
- `peer`: `PeerProcessorPlugin` implementations and `TaskProcessor` classes that
  execute assigned tasks. Heavy processor-only dependencies belong here.

Add the domain parent POM to the root reactor, then add its role modules to the
domain parent. Add artifacts to root dependency management if they need to be
referenced by runtime modules.

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
- `validateSubmission(JobSubmitMessage)` rejects bad parameters, missing
  payloads, malformed payload objects, unsupported file extensions or formats,
  and invalid encoded data before tasks are created.
- `createJob(JobSubmitMessage, String requesterId)` returns a job that can split
  the submission into task units and aggregate typed results.

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
requests because TCP and RabbitMQ submitters can send raw protocol messages.

Jobs should extend `EmbarrassinglyParallelJob<T, R>` when the work can be split
into independent tasks. A job should:

- Build deterministic task IDs while initializing tasks.
- Convert raw payloads and results through typed model objects.
- Accept results only through `recordResult`; the base class already rejects
  stale results from peers that no longer own the task.
- Aggregate final results in deterministic order when ordering matters.
- Include the same task type and parameter in `TaskAssignMessage` that the peer
  processor expects.

## Client Plugin

The client module owns local input and output behavior for command-line
submitters and the JavaFX GUI.

Implement `client.ClientJobPlugin`:

- `taskType()` returns the shared task type constant.
- `displayName()` is the human-readable GUI label.
- `supportedInputExtensions()` controls file chooser filtering and CLI
  validation expectations.
- `parameterOptions()` and `defaultParameter()` define the task option set.
- `buildPayloads(List<Path>, String)` reads local inputs and returns payload
  objects suitable for JSON serialization.
- `saveResults(List<Object>, Path)` writes final results into the selected
  output directory.

Register the provider in:

```text
plugins/<domain>/client/src/main/resources/META-INF/services/client.ClientJobPlugin
```

Use `PayloadLimits` when reading local inputs and writing results:

- `maxTasksPerJob()` for input count.
- `maxInputBytes()` for each local input file.
- `maxJobPayloadBytes()` for aggregate encoded job payload size.
- `maxResultBytes()` for decoded result payloads written to disk.

Use `SafeFileNames.safeOutputPath(...)` or an equivalent safe path strategy when
writing files from result-provided names. Do not trust result filenames to stay
inside the selected output directory, and do not silently overwrite duplicate
result names unless the plugin's behavior explicitly requires it.

## Peer Plugin

The peer module owns task execution.

Implement `peer.engine.PeerProcessorPlugin`:

- `taskType()` returns the shared task type constant.
- `createProcessor()` returns a `TaskProcessor<?>` for that type.

Register the provider in:

```text
plugins/<domain>/peer/src/main/resources/META-INF/services/peer.engine.PeerProcessorPlugin
```

The processor receives a `TaskAssignMessage` and returns a JSON-serializable
result object. Convert `task.getPayload()` into the typed model object used by
the server job. Throw an exception when the task cannot be processed; the peer
execution engine converts failures into failed `TASK_RESULT` messages.

Keep native, media, ML, or other heavy execution dependencies in the peer module
when submitters do not need them.

## Runtime Classpaths

Adding a plugin is not enough; the right role artifact must be present on the
right runtime classpath.

- `taskflow-coordinator` should depend on the plugin's `server` artifact at
  runtime. It should not need client or peer artifacts.
- `taskflow-peer` `combined-runtime` should include both `client` and `peer`
  artifacts when a command-line peer can submit jobs and execute tasks.
- `taskflow-peer` `submitter-runtime` should include only `client` artifacts.
- `taskflow-peer` `executor-runtime` should include only `peer` artifacts.
- `taskflow-gui` follows the same `combined-runtime`, `submitter-runtime`, and
  `executor-runtime` split as `taskflow-peer`.

The coordinator and peer shaded JARs already use Maven Shade's
`ServicesResourceTransformer`, so service files from plugin artifacts are
merged into packaged runtime JARs.

## Focused Tests

Add tests in the plugin role modules before relying on the new type in a demo.
Use the conversion and text tests as templates.

Server module tests should cover:

- `ServiceLoader.load(TaskPlugin.class)` discovers the expected task type.
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
- Result saving writes the expected files, stays inside the selected output
  directory, handles duplicate names safely, and enforces result-size limits when
  the result contains file data.

Peer module tests should cover:

- `ServiceLoader.load(PeerProcessorPlugin.class)` discovers the expected task
  type.
- The processor returns the expected typed result for a representative
  `TaskAssignMessage`.
- Processor failure behavior is tested directly when invalid task payloads or
  unsupported parameters are realistic.

If a new plugin changes runtime dependencies or role classpaths, run dependency
tree checks for the affected modules in addition to focused tests.

## Add A New Task Type Without Touching Core

Use this checklist for each new task type:

1. Add a task type constant and shared payload/result records in
   `plugins/<domain>/model`.
2. Add a server `TaskPlugin`, job, task unit, validation helper, and
   `META-INF/services/server.job.TaskPlugin` entry.
3. Add a client `ClientJobPlugin`, payload/result file handling, payload-limit
   checks, safe output naming, and `META-INF/services/client.ClientJobPlugin`
   entry.
4. Add a peer `PeerProcessorPlugin`, processor implementation, and
   `META-INF/services/peer.engine.PeerProcessorPlugin` entry.
5. Wire the domain modules into Maven reactor and dependency management.
6. Add server artifacts to `taskflow-coordinator` runtime dependencies.
7. Add client and peer artifacts to `taskflow-peer` and `taskflow-gui` profiles
   according to `combined-runtime`, `submitter-runtime`, and `executor-runtime`.
8. Add focused server, client, and peer tests for discovery, validation,
   payload creation, result saving, processing, and aggregation.
9. Run the focused tests for the new plugin modules.
10. Run `git diff --check`; run broader Maven and dependency-tree gates if the
    plugin changed runtime classpaths or packaged runtime behavior.

If any step requires changing scheduler, transport, GUI, or peer engine code,
stop and check whether the new job type is missing a plugin-owned contract or
whether the framework needs an explicit new extension point.
