# 0005: Use Per-Job Result Ownership And Defer Full Account Authentication

Status: Accepted

Date: 2026-07-04

## Context

TaskFlow needs to prevent arbitrary result replay while keeping the project
focused on task orchestration, peer scheduling, plugin contracts, and transport
behavior. A full user/account system would require login sessions, identities,
roles, credential storage, and operational policy that are outside the current
framework scope.

The implemented result-ownership model already supports requester tokens,
optional requester public keys, signed submissions, signed result requests, and
local GUI token/key persistence.

## Decision

TaskFlow uses per-job requester tokens plus local requester signing keys when
present. The coordinator persists token hashes and requester public keys, not
raw tokens or private keys.

Full user/account authentication, role-based authorization, login sessions, and
a credential vault are deferred unless the project scope expands.

## Consequences

- Result requests must present the matching requester token.
- Identity-bound jobs must also present the matching public key and a valid
  signature over the request fields.
- JavaFX stores raw requester tokens and its local signing key in a
  user-profile file; POSIX owner-only permissions are attempted when supported.
- Transport credentials, such as RabbitMQ username/password and future TLS
  settings, are separate from TaskFlow result ownership.
- Public docs must not claim account authentication, role-based authorization,
  replay-proof sessions, or secret-vault behavior.

## Evidence

- `docs/EXECUTION_GUARANTEES.md`
- `docs/PEER_IDENTITY.md`
- `docs/PROTOCOL_COMPATIBILITY.md`
- `taskflow-spi/src/main/java/protocol/RequesterTokens.java`
- `taskflow-spi/src/main/java/protocol/RequesterIdentity.java`
- `taskflow-core/src/main/java/server/scheduler/JobResultRequestAuthorizer.java`
- `taskflow-gui/src/main/java/gui/FileGuiRequesterTokenStore.java`

## Related Documents

- [Execution Guarantees](../EXECUTION_GUARANTEES.md)
- [Peer Identity](../PEER_IDENTITY.md)
- [Protocol Compatibility](../PROTOCOL_COMPATIBILITY.md)
