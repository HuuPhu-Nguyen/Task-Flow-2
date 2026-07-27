# 0011: Use Object Storage For Large Payloads

Status: Accepted

Date: 2026-07-22

Scope: Frozen end-state for Phases 0–8; MinIO/S3 support is planned in Phase 5
and the current local/shared-filesystem reference path remains transitional.

## Context

RabbitMQ is a control plane, not an unlimited binary transport. Inline image or
video bodies increase broker memory, persistence, redelivery, and latency costs.
The current local-file reference option also assumes equivalent shared paths
across participant processes, which is not a valid general distributed payload
contract.

Large inputs and outputs need location-independent references, streaming,
integrity metadata, attempt ownership, and safe cleanup after crashes.

## Decision

Large payloads use a MinIO/S3-compatible object store behind a core-owned
`ObjectStore` port. Messages carry TaskFlow-generated object keys plus content
length, SHA-256 digest, and content type. Small payloads may remain inline only
below a documented configured limit.

Inputs and downloads are streamed and verified before processing. Outputs are
uploaded under immutable attempt-specific keys containing job, task, attempt
number, and assignment ID. The SQLite pointer committed by the current fenced
assignment is authoritative; object upload alone is not result commitment.
Bounded garbage collection removes old non-authoritative staged outputs after a
safety window.

## Alternatives Considered

- **Put arbitrary binary bodies in RabbitMQ:** rejected because it couples the
  control plane to large-payload memory, persistence, and redelivery cost.
- **Use shared local filesystem paths as the distributed protocol:** rejected
  because paths and mounts are machine-specific and do not establish portable
  ownership or availability.
- **Store binary BLOBs in SQLite:** rejected because it mixes coordinator
  control-state writes with high-volume media storage and streaming concerns.
- **Let plugins choose unvalidated external URLs:** rejected because ownership,
  credentials, length, digest, and cleanup would be unenforced.

## Consequences

- MinIO/S3 becomes a runtime dependency for large-payload distributed jobs
  after Phase 5; credentials remain external configuration.
- RabbitMQ messages stay bounded and primarily contain control metadata.
- Every object read must enforce length and SHA-256 integrity (I9).
- Multiple attempts may leave staged objects, but only one database reference
  can become authoritative.
- Cleanup requires a safety window, idempotent deletion, failure recording, and
  bounded batches.
- The current local/shared-file option must not be presented as the Phase 5
  distributed data plane.

## Conditions That Would Invalidate This Decision

A replacement ADR is required if measured supported workloads are permanently
small enough that no external payload tier is needed, or if security,
residency, platform, or lifecycle requirements cannot be met by the selected
S3-compatible service.

Any replacement must preserve bounded broker messages, portable references,
streaming, length/digest verification, attempt-specific ownership, and orphan
cleanup. A preference for embedding payloads or avoiding one local dependency
does not waive those requirements.

## Evidence And Implementation Status

- [Payload storage — current transitional behavior](../PAYLOAD_STORAGE.md)
- [Guarantees and non-goals](../GUARANTEES.md)
- [Failure model](../FAILURE_MODEL.md)
- TF-0501 implements the framework-owned
  [`ObjectStore`](../../taskflow-spi/src/main/java/objectstore/ObjectStore.java)
  port, validated
  [`ObjectReference`](../../taskflow-spi/src/main/java/objectstore/ObjectReference.java)
  metadata, and isolated
  [`MinioObjectStore`](../../taskflow-objectstore-minio/src/main/java/objectstore/minio/MinioObjectStore.java)
  adapter.
- One inherited
  [`ObjectStoreContractTest`](../../taskflow-objectstore-minio/src/test/java/objectstore/minio/ObjectStoreContractTest.java)
  runs against both the in-memory fake and a real Testcontainers MinIO service.
- TF-0502 through TF-0506 still own runtime environment wiring, protocol
  references, end-to-end integrity verification, attempt-specific authoritative
  output pointers, and garbage collection. The TF-0501 metadata contract alone
  does not close I9.

## Related Documents

- [ADR 0009: RabbitMQ sole supported transport](0009-rabbitmq-sole-supported-transport.md)
- [ADR 0010: At-least-once execution and fenced results](0010-at-least-once-generation-fenced-results.md)
