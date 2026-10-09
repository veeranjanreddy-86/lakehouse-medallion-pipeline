# Security Policy

## Scope

This is a portfolio / reference project. It processes **synthetic data only** and
contains no credentials, customer data or proprietary code. It is not intended to be
deployed as-is against real financial data.

## Reporting a vulnerability

Please report suspected vulnerabilities privately via GitHub's
[private vulnerability reporting](https://github.com/veeranjanreddy-86/lakehouse-medallion-pipeline/security/advisories/new)
rather than a public issue. Include steps to reproduce and the affected version or
commit. You can expect an acknowledgement within a few days.

## Guidance if you adapt this for real data

- Never commit credentials. Use a secret manager (e.g. Databricks secret scopes,
  cloud KMS-backed stores) and environment-specific configuration.
- Treat customer attributes (names, e-mail) as PII: apply column-level access
  control / masking (e.g. Unity Catalog), and avoid writing raw PII to logs or the
  quarantine table without the same protections as the source.
- Pin and scan dependencies (`pip-audit`, Dependabot) and keep the Spark/Java runtime
  patched.
- The bundled `Dockerfile` runs as a non-root user; keep it that way.
