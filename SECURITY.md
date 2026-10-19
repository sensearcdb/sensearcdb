# Security Policy

## Reporting a vulnerability

Please do **not** open a public issue for security-sensitive reports. Instead, report vulnerabilities privately to the maintainers via GitHub's **Report a vulnerability** flow (Security → Advisories → Report a vulnerability) on this repository.

We will acknowledge receipt and aim to respond within a reasonable timeframe.

## Scope & security posture of this repository

This repository is the **open-source single-node core**. Be aware of the following by design:

- **No authentication**: the core runs without auth; a trusted `x-tenant-id` is passed through (defaults to tenant `0`). It is intended to be deployed behind your own trusted boundary.
- **No encryption at rest (TDE)**: the open-source build does not include transparent data encryption.
- **No transport encryption**: gRPC/REST endpoints are plaintext in the open-source build.

These are deliberate Open-Core boundaries. The commercial edition provides the full security & compliance stack (SM2/SM3/SM4 国密, mTLS, RBAC, audit logging, TDE, rate limiting).

## Deployment guidance

- Deploy the open-source core only inside a trusted network, or terminate TLS and enforce access control at a reverse proxy / gateway in front of it.
- For production environments that require authentication, encryption, or compliance, use the commercial edition.
