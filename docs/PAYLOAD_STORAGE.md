# Payload Storage

RabbitMQ carries TaskFlow control metadata, not large image/video bodies.
Conversion submitters keep small files inline only below an exclusive configured
limit. At or above that limit they upload the input to MinIO/S3-compatible
object storage and submit a portable `ObjectReference`.

Local/shared-filesystem payload references are no longer part of the
distributed protocol. A participant never resolves a path supplied by another
machine.

## Current Scope

The Phase 5 input path is implemented for built-in image and video conversion:

- submitters upload large inputs under immutable TaskFlow-generated
  `taskflow/inputs/<uuid>` keys;
- `JOB_SUBMIT` and `TASK_ASSIGN` carry only file metadata and
  `ObjectReference(key, contentLength, sha256, contentType)`;
- executor processors open their own configured object-store client and
  download by `key`;
- requester result handlers can download an object-referenced result by key;
- protocol validation recursively rejects malformed/oversized references,
  legacy local-file reference metadata, malformed Base64, and inline file
  payloads at or above the configured limit.

Text analysis and plugin-owned semantic JSON remain inline. Image/video outputs
remain inline only below the same limit. A larger conversion output fails
instead of placing a large body on RabbitMQ; TF-0505 owns immutable
attempt-specific output keys and the authoritative SQLite result pointer.

## Object-store Boundary and Runtime Provider

`taskflow-spi` owns:

- `ObjectStore`, the streaming data-plane port;
- `ObjectReference`, the portable wire metadata;
- `ObjectStoreProvider`, the runtime construction boundary; and
- `ObjectStores`, which requires exactly one provider discovered through
  `ServiceLoader`.

`taskflow-objectstore-minio` supplies the provider and contains all MinIO SDK
usage. Command-line and JavaFX participant runtimes include that adapter;
coordinator/core/persistence sources and the coordinator runtime remain
MinIO-free.

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

For participant processes launched on the host, point TaskFlow at that service:

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
`base64Data` file payload. It defaults to `8388608` bytes:

- raw size `< limit`: inline Base64 is allowed;
- raw size `>= limit`: the conversion client must use object storage;
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

Keys reject path traversal, URI/path-shaped segments, backslashes, empty
segments, and names outside `taskflow/`. The file name is display/output
metadata and is never interpreted as an object-store or remote filesystem
location.

The removed form used `payloadReference` with `storageType`, `location`,
`sizeBytes`, and `sha256`. Shared validation rejects that metadata. Conversion
participants using the old and new referenced-media shapes are not compatible
and must be upgraded together.

## Ownership, Failure, and Remaining Integrity Work

Input objects are immutable staging data, not authoritative coordinator state.
If payload building fails after uploads, the submitter deletes objects from
that build best-effort. An object may remain after an uncertain broker publish,
process crash, or failed cleanup; TF-0506 owns bounded orphan collection.

An unavailable store or missing object fails payload building, execution, or
result saving through the existing failure path. No SQLite job/task state is
created when submission payload construction fails before publication.

Submitters currently calculate and carry SHA-256 metadata, and readers enforce
configured upper bounds while downloading. They do not yet prove that the
downloaded stream has the exact declared length and digest before processor
invocation. TF-0504 owns that end-to-end streaming verification and explicit
corrupt-byte rejection; metadata presence and a successful MinIO round trip do
not close invariant I9.

`MinioObjectStoreContractTest#separateConversionParticipantsExchangeInputOnlyByPortableObjectKey`
uploads with one provider/client, serializes the payload, and processes it with
a separately constructed provider/client against real Testcontainers MinIO.
The test asserts that no local path fields or submitter path cross the wire.
