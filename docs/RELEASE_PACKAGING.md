# Release Packaging

This document defines TaskFlow's current release packaging strategy. It is about
build outputs and runtime classpaths; it does not promote RabbitMQ beyond the
support status in `docs/RABBITMQ_SCOPE.md`.

## Decision

TaskFlow publishes role-specific Maven package outputs instead of one default
participant artifact for every role. Existing `taskflow-peer` artifact names
remain compatibility names for the command-line participant runtime.

- The sole-authority coordinator is a shaded server jar:
  `taskflow-coordinator-<version>-coordinator-runtime.jar`.
- The command-line participant has three shaded jars:
  `taskflow-peer-<version>-combined-runtime.jar`,
  `taskflow-peer-<version>-submitter-runtime.jar`, and
  `taskflow-peer-<version>-executor-runtime.jar`.
- The JavaFX participant is a classpath package, not a shaded fat jar, because
  JavaFX dependencies are platform-specific and should stay visible.
- Plugin bundles stay as role-split Maven artifacts under `plugins/<domain>`.
- Docker Compose remains the broker-backed demo distribution, not the only
  release packaging mechanism.
- Every supported runtime package uses `taskflow-transport-rabbitmq`; no legacy
  socket transport module or implementation is included in the default reactor
  or release classpaths.

The default `combined-runtime` participant is a dual-role convenience and demo
package. Use the requester-only `submitter-runtime` profile or the executor-only
profile for smaller deployment classpaths. The requester package intentionally
omits peer processor artifacts and native media dependencies such as
JavaCV/FFmpeg.

## Package Commands

Build the coordinator package:

```powershell
.\mvnw.cmd --batch-mode --no-transfer-progress -pl taskflow-coordinator -am -DskipTests package
```

Output:

```text
taskflow-coordinator/target/taskflow-coordinator-1.0-SNAPSHOT-coordinator-runtime.jar
```

Build command-line participant packages:

```powershell
.\mvnw.cmd --batch-mode --no-transfer-progress -pl taskflow-peer -Pcombined-runtime -am -DskipTests package
.\mvnw.cmd --batch-mode --no-transfer-progress -pl taskflow-peer -Psubmitter-runtime -am -DskipTests package
.\mvnw.cmd --batch-mode --no-transfer-progress -pl taskflow-peer -Pexecutor-runtime -am -DskipTests package
```

Outputs:

```text
taskflow-peer/target/taskflow-peer-1.0-SNAPSHOT-combined-runtime.jar
taskflow-peer/target/taskflow-peer-1.0-SNAPSHOT-submitter-runtime.jar
taskflow-peer/target/taskflow-peer-1.0-SNAPSHOT-executor-runtime.jar
```

Build a JavaFX participant jar for the selected role profile:

```powershell
.\mvnw.cmd --batch-mode --no-transfer-progress -pl taskflow-gui -Pcombined-runtime -am -DskipTests package
.\mvnw.cmd --batch-mode --no-transfer-progress -pl taskflow-gui -Psubmitter-runtime -am -DskipTests package
.\mvnw.cmd --batch-mode --no-transfer-progress -pl taskflow-gui -Pexecutor-runtime -am -DskipTests package
```

The GUI jar is:

```text
taskflow-gui/target/taskflow-gui-1.0-SNAPSHOT.jar
```

For a standalone GUI folder, copy runtime dependencies for the selected role
profile into a `lib` directory:

```powershell
.\mvnw.cmd --batch-mode --no-transfer-progress -pl taskflow-gui -Pcombined-runtime -DincludeScope=runtime "-DoutputDirectory=target/distributions/gui-combined/lib" dependency:copy-dependencies
Copy-Item -LiteralPath taskflow-gui\target\taskflow-gui-1.0-SNAPSHOT.jar -Destination target\distributions\gui-combined\
```

Launch that folder with JavaFX on the module path:

```powershell
java --module-path target\distributions\gui-combined\lib --add-modules javafx.controls,javafx.fxml -cp "target\distributions\gui-combined\taskflow-gui-1.0-SNAPSHOT.jar;target\distributions\gui-combined\lib\*" gui.PeerAppLauncher
```

Use the same pattern with `-Psubmitter-runtime` or `-Pexecutor-runtime` when
building narrower GUI distributions.

## Runtime Package Roles

Coordinator package:

- Includes core scheduler code, SQLite persistence, RabbitMQ transport, and
  coordinator-side server plugin artifacts.
- Excludes client plugin artifacts, peer processor artifacts, JavaFX, and
  JavaCV/FFmpeg processor dependencies.

Command-line participant packages (`taskflow-peer` artifact):

- `combined-runtime` includes both requester/client plugins and executor/peer
  processors. It is the default dual-role command-line participant package
  profile for local demos and Docker Compose.
- `submitter-runtime` is the retained profile name for the requester role and
  includes client plugins only. Use it for one-shot RabbitMQ submissions or
  DLQ commands that should not carry processor/native dependencies.
- `executor-runtime` includes peer processors only. Use it for executor-only
  participant nodes that should execute assignments without carrying local
  payload creation and result-handling plugins.

JavaFX participant packages:

- Follow the same `combined-runtime`, `submitter-runtime`, and
  `executor-runtime` profile split as the command-line participant.
- Stay classpath-based so JavaFX and platform-specific runtime dependencies are
  inspectable and replaceable by the packager.
- Use `gui.PeerAppLauncher` as the package entry point.

Plugin bundles:

- Keep shared model classes in `plugins/<domain>/model`.
- Add coordinator behavior through `plugins/<domain>/server`.
- Add requester-role local submit/result behavior through
  `plugins/<domain>/client`.
- Add executor-role behavior and heavy processor dependencies through
  `plugins/<domain>/peer`.
- Wire only the needed role artifact into each runtime package.

## Package Smoke Checks

Each package exposes a help path that exits without opening sockets, connecting
to RabbitMQ, or opening a JavaFX window:

```powershell
java -jar taskflow-coordinator\target\taskflow-coordinator-1.0-SNAPSHOT-coordinator-runtime.jar --help
java -jar taskflow-peer\target\taskflow-peer-1.0-SNAPSHOT-combined-runtime.jar --help
java -jar taskflow-peer\target\taskflow-peer-1.0-SNAPSHOT-submitter-runtime.jar --help
java -jar taskflow-peer\target\taskflow-peer-1.0-SNAPSHOT-executor-runtime.jar --help
java -cp taskflow-gui\target\taskflow-gui-1.0-SNAPSHOT.jar gui.PeerAppLauncher --help
```

The coordinator package also exposes the operator inspection command:

```powershell
java -jar taskflow-coordinator\target\taskflow-coordinator-1.0-SNAPSHOT-coordinator-runtime.jar status summary
java -jar taskflow-coordinator\target\taskflow-coordinator-1.0-SNAPSHOT-coordinator-runtime.jar status jobs 20
java -jar taskflow-coordinator\target\taskflow-coordinator-1.0-SNAPSHOT-coordinator-runtime.jar status peers 20
java -jar taskflow-coordinator\target\taskflow-coordinator-1.0-SNAPSHOT-coordinator-runtime.jar status outbox 20
java -jar taskflow-coordinator\target\taskflow-coordinator-1.0-SNAPSHOT-coordinator-runtime.jar status queues
java -jar taskflow-coordinator\target\taskflow-coordinator-1.0-SNAPSHOT-coordinator-runtime.jar status dlq 20
```

The GUI smoke command intentionally uses only the GUI jar. It verifies the
package entry point and help path; full GUI launch still requires the JavaFX
runtime dependencies on the module path as shown above.

After packaging, verify that no removed runtime classes entered a release jar:

```powershell
$runtimeJars = @(
    "taskflow-coordinator\target\taskflow-coordinator-1.0-SNAPSHOT-coordinator-runtime.jar",
    "taskflow-peer\target\taskflow-peer-1.0-SNAPSHOT-combined-runtime.jar",
    "taskflow-peer\target\taskflow-peer-1.0-SNAPSHOT-submitter-runtime.jar",
    "taskflow-peer\target\taskflow-peer-1.0-SNAPSHOT-executor-runtime.jar",
    "taskflow-gui\target\taskflow-gui-1.0-SNAPSHOT.jar"
)
foreach ($runtimeJar in $runtimeJars) {
    $legacy = (& jar tf $runtimeJar) |
        Select-String -CaseSensitive '^(server/(handler/PeerHandler|transport/TcpPeerConnection|scheduler/PeerRegistrySchedulerOutput)|messaging/(MessageDispatcher|MessageFactory|MessageHandler|SafeJsonWriter|handlers/(PingHandler|PongHandler))|transport/TransportConnection|gui/(GuiTransportMode|TcpCoordinatorConnection|TcpJobSubmissionClient))(\$.*)?\.class$'
    if ($legacy) {
        throw "Legacy transport class found in $runtimeJar`: $legacy"
    }
}
```

## Dependency Gates

Run dependency-tree checks whenever package roles change.

Required checks for the current package strategy:

- The root reactor and coordinator, command-line participant, and GUI packages
  must declare RabbitMQ as their only runtime transport dependency.
- `taskflow-coordinator` must not depend on client plugins, peer plugins,
  JavaFX, or JavaCV/FFmpeg.
- `taskflow-peer -Psubmitter-runtime` and
  `taskflow-gui -Psubmitter-runtime` must not depend on peer plugin artifacts,
  `javacv-platform`, or `pdfbox`.
- `taskflow-peer -Pexecutor-runtime` and
  `taskflow-gui -Pexecutor-runtime` must not depend on client plugin artifacts.
- The example plugin must remain outside coordinator, command-line participant,
  and GUI participant runtime packages unless it is deliberately promoted from
  authoring template to supported runtime plugin.

## Deferred Packaging

The following are intentionally deferred until there is a concrete release
target:

- OS installers or `jpackage` images.
- Platform-specific JavaFX installers.
- Platform-specific JavaCV executor packages.
- Published container images beyond the local Docker Compose demo.
- A plugin marketplace or external plugin download mechanism.
