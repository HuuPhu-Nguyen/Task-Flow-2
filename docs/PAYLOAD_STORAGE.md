# Payload Storage

RabbitMQ carries TaskFlow control metadata, not large image/video bodies.
Conversion submitters keep small files inline only below an exclusive configured
limit. At or above that limit they upload the input to MinIO/S3-compatible
object storage and submit a portable `ObjectReference`.

Local/shared-filesystem payload references are no longer part of the
distributed protocol. A participant never resolves a path supplied by another
machine.

## Current Scope

The Phase 5 object-backed path is implemented for built-in image and video
conversion:

- submitters upload large inputs under immutable TaskFlow-generated
  `taskflow/inputs/<uuid>` keys;
- `JOB_SUBMIT` and `TASK_ASSIGN` carry only file metadata and
  `ObjectReference(key, contentLength, sha256, contentType)`;
- executor processors open their own configured object-store client and
  download by `key`;
- executors conditionally create large conversion outputs under the exact
  assignment-owned key and return its `ObjectReference` in `TASK_RESULT`;
- requester result handlers can download an object-referenced result by key;
- protocol validation recursively rejects malformed/oversized references,
  legacy local-file reference metadata, malformed Base64, and inline file
  payloads at or above the configured limit.

Text analysis and plugin-owned semantic JSON remain inline. Image/video outputs
remain inline only below the same limit. At or above it, output bytes are
staged at:

```text
taskflow/jobs/{jobId}/tasks/{taskId}/attempts/{attemptNumber}/{assignmentId}/output
```

The key is created atomically only when absent. Re-execution of the same
assignment reuses an existing object only when its length and SHA-256 match;
different content is a permanent integrity failure and never overwrites the
key. Different attempts may therefore stage independent objects.

## Object-store Boundary and Runtime Provider

`taskflow-spi` owns:

- `ObjectStore`, the streaming data-plane port;
- `ObjectReference`, the portable wire metadata;
- `ObjectStoreProvider`, the runtime construction boundary; and
- `ObjectStores`, which requires exactly one provider discovered through
  `ServiceLoader`.

`taskflow-objectstore-minio` supplies the provider and contains all MinIO SDK
usage. Command-line and JavaFX participant runtimes include that adapter for
uploads/downloads. The coordinator runtime includes it for orphan-output
collection. Coordinator/core/persistence source remains independent from the
MinIO SDK, and SQLite—not MinIO—remains the authority.

The MinIO provider reads:

| Environment variable | System property | Default |
|---|---|---|
| `TASKFLOW_MINIO_ENDPOINT` | `taskflow.minioEndpoint` | `http://localhost:9000` |
| `TASKFLOW_MINIO_ACCESS_KEY` | `taskflow.minioAccessKey` | required |
| `TASKFLOW_MINIO_SECRET_KEY` | `taskflow.minioSecretKey` | required |
| `TASKFLOW_MINIO_BUCKET` | `taskflow.minioBucket` | `taskflow` |

Credentials have no source-controlled fallback. Missing provider/credentials do
not affect small inline text/testing payloads, but a large conversion input or
referenced download fails clearly; TaskFlow never falls back to a local path.
Bucket creation remains an operator/deployment action.

## Local MinIO Environment

The opt-in `object-store` Compose profile provides a pinned MinIO server,
persistent `taskflow-minio-data` volume, native health check, and idempotent
private-bucket initializer. Export server credentials before starting it:

```powershell
$env:MINIO_ROOT_USER = "<local-access-key>"
$env:MINIO_ROOT_PASSWORD = "<local-secret-key>"
$env:TASKFLOW_MINIO_BUCKET = "taskflow"
docker compose --profile object-store up --detach minio minio-init
docker compose --profile object-store ps --all minio minio-init
```

For participant and coordinator processes launched on the host, point TaskFlow
at that service:

```powershell
$env:TASKFLOW_MINIO_ENDPOINT = "http://localhost:9000"
$env:TASKFLOW_MINIO_ACCESS_KEY = $env:MINIO_ROOT_USER
$env:TASKFLOW_MINIO_SECRET_KEY = $env:MINIO_ROOT_PASSWORD
$env:TASKFLOW_MINIO_BUCKET = "taskflow"
```

Compose participant services receive the same credentials as TaskFlow runtime
variables and use `http://minio:9000` inside the Compose network. The tiny
default demo inputs remain inline; start the `object-store` profile when testing
large conversion inputs.

The initializer and restart checks remain repeatable:

```powershell
docker compose --profile object-store run --rm --no-deps minio-init
docker compose --profile object-store restart minio
docker compose --profile object-store ps minio
docker compose --profile object-store run --rm --no-deps minio-init
```

Ordinary removal retains the named volume:

```powershell
docker compose --profile object-store rm --stop --force minio minio-init
```

Do not add `--volumes` unless deleting local object data is intentional.

## Inline Limit and Reference Format

`TASKFLOW_MAX_INLINE_PAYLOAD_BYTES` (system property
`taskflow.maxInlinePayloadBytes`) is the exclusive raw-byte ceiling for a
`base64Data` input or output file payload. It defaults to `8388608` bytes:

- raw size `< limit`: inline Base64 is allowed;
- raw size `>= limit`: conversion inputs and outputs must use object storage;
- value `0`: conversion file inlining is disabled.

This is separate from:

- `TASKFLOW_MAX_INPUT_BYTES`, default `33554432`, the inclusive per-input
  reference/content bound;
- `TASKFLOW_MAX_JOB_PAYLOAD_BYTES`, default `67108864`, the inclusive exact
  UTF-8 JSON envelope bound; and
- `TASKFLOW_MAX_RESULT_BYTES`, default `67108864`, the inclusive per-result
  bound.

An object-backed conversion input serializes as:

```json
{
  "fileName": "sample.png",
  "objectReference": {
    "key": "taskflow/inputs/550e8400-e29b-41d4-a716-446655440000",
    "contentLength": 12345678,
    "sha256": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    "contentType": "image/png"
  }
}
```

An object-backed conversion result uses the same `FilePayload` shape inside
protocol-v2 `TASK_RESULT.resultPayload`. Its key must exactly match the outer
`jobId`, `taskId`, `attemptNumber`, and `assignmentId`. A mismatch is rejected
as `invalid_task_output_reference` before scheduler handling and is checked
again inside the SQLite result transaction.

Keys reject path traversal, URI/path-shaped segments, backslashes, empty
segments, and names outside `taskflow/`. The file name is display/output
metadata and is never interpreted as an object-store or remote filesystem
location.

The removed form used `payloadReference` with `storageType`, `location`,
`sizeBytes`, and `sha256`. Shared validation rejects that metadata. Conversion
participants using the old and new referenced-media shapes are not compatible
and must be upgraded together.

## Ownership, Failure, and Integrity

Input objects are immutable staging data, not authoritative coordinator state.
If payload building fails after uploads, the submitter deletes objects from
that build best-effort. An object may remain after an uncertain broker publish,
process crash, or failed cleanup. These `taskflow/inputs/` objects are
classified as referenced input, but TF-0506 deliberately does not infer their
retention from coordinator task state and does not delete them automatically.

Attempt output objects are also staging data until the exact assignment result
transaction commits. That transaction changes the task to `COMPLETED`, stores
the result (including its reference) in `tasks.result_payload_json`, closes the
exact running attempt as `SUCCEEDED`, and may mark the parent `FINALIZING`.
SQLite commit is the authority boundary:

- crash before commit leaves a non-authoritative staged object;
- crash after commit recovers the stored pointer from SQLite;
- a late attempt result cannot replace the committed pointer; and
- no S3 rename, copy promotion, or object upload is treated as commitment.

## Orphan-output lifecycle and collection

Stored payloads have these lifecycle classifications:

- **referenced input:** immutable `taskflow/inputs/` data; outside the current
  automatic deletion policy;
- **staged attempt output:** an exact attempt key whose upload alone has no
  authority;
- **authoritative output:** a key present in the matching task's committed
  `tasks.result_payload_json`;
- **orphan candidate:** an exact attempt output old enough for the safety
  window whose assignment is neither active nor authoritative; and
- **deleted:** an orphan candidate for which idempotent deletion succeeded,
  including when the object was already absent.

The coordinator scans only `taskflow/jobs/` in bounded lexical pages. Each
listing entry carries the immutable object's real creation timestamp. This is
important: using attempt age would let a stale executor upload a new object
after the attempt was closed and lose the safety window.

For every old exact output key, SQLite classifies the complete job/task/attempt/
assignment identity under the same synchronized state boundary as result
commit. `AUTHORITATIVE` is preserved first; the exact current `ASSIGNED`
generation is preserved as `ACTIVE`; only the remaining key is an
`ORPHAN_CANDIDATE`. Once a generation is no longer active, the existing result
fence prevents it from becoming authoritative later.

Schema v13 stores failed deletes in `orphan_output_gc_failures`, including
first/last failure time, saturated attempt count, and bounded last error. Every
later batch reclassifies the oldest failures before retrying. Delete is
idempotent. A crash before delete leaves the object discoverable; a crash after
delete but before failure-row removal repeats a harmless delete; and a crash
after a failed delete but before its row is written still leaves the object for
the next bounded lexical scan.

| Environment variable | System property | Default |
|---|---|---|
| `TASKFLOW_ORPHAN_OUTPUT_GC_ENABLED` | `taskflow.orphanOutputGcEnabled` | `true` |
| `TASKFLOW_ORPHAN_OUTPUT_GC_SAFETY_WINDOW_MS` | `taskflow.orphanOutputGcSafetyWindowMs` | `86400000` (24 hours) |
| `TASKFLOW_ORPHAN_OUTPUT_GC_INTERVAL_MS` | `taskflow.orphanOutputGcIntervalMs` | `300000` (5 minutes) |
| `TASKFLOW_ORPHAN_OUTPUT_GC_BATCH_SIZE` | `taskflow.orphanOutputGcBatchSize` | `100` |

The interval and safety window must be positive. Batch size must be `2..1000`
so every pass can reserve work for durable retries and new discovery. A store
outage stops external work for the current pass and waits for the fixed delay;
there is no immediate retry loop. Missing provider/credentials disable only GC
for that coordinator run, with a structured reason, so small-inline scheduling
can continue. Set `TASKFLOW_ORPHAN_OUTPUT_GC_ENABLED=false` only when an
external lifecycle owner is deliberately responsible for cleanup.

An unavailable store or missing object fails payload building, execution, or
result saving through the existing failure path. No SQLite job/task state is
created when submission payload construction fails before publication.

Submitters stream each object-backed input once to calculate its exact length
and SHA-256 metadata. The object-store upload streams it again through a
verifier and deletes the object best-effort if those uploaded bytes no longer
match the reference. Executor input readers and requester result handlers read
through the same bounded verifier and accept bytes only after exact length and
SHA-256 checks. Truncated, extended, and same-length corrupt content therefore
fails before media decoding or final output-file creation.

`PayloadIntegrityException` is a permanent immutable-object failure. The
executor emits a protocol-v2 `TASK_RESULT` with
`failureClassification: "PERMANENT_PAYLOAD_INTEGRITY"`. A current coordinator
atomically closes that exact assignment as terminal on its first accepted
failure, emits the integrity events/counter, and creates no replacement
assignment. Missing classification retains the earlier retryable v2 behavior,
so executor and coordinator participants must be upgraded together to obtain
the no-retry guarantee.

`MinioObjectStoreContractTest#separateConversionParticipantsExchangeInputOnlyByPortableObjectKey`
uploads with one provider/client, serializes the payload, and processes two
attempts with a separately constructed provider/client against real
Testcontainers MinIO. The test asserts that no local path fields or submitter
path cross the wire and that both attempt outputs exist at distinct exact keys.
The inherited object-store contract proves conditional create never replaces
an existing key against both the in-memory fake and real MinIO.
`MinioObjectStoreContractTest#corruptObjectBytesAreRejectedBeforeImageProcessing`
overwrites that portable object with same-length corrupt bytes and proves that
the real MinIO download is rejected before image processing.
