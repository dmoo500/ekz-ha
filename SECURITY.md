# Security Policy

## Supported Versions

Only the latest release receives security updates.

| Version | Supported          |
| ------- | ------------------ |
| latest  | :white_check_mark: |
| older   | :x:                |

## Reporting a Vulnerability

Please **do not** open a public GitHub issue for security vulnerabilities.

Use [GitHub Private Vulnerability Reporting](https://github.com/dmoo500/ekz-ha/security/advisories/new) to report issues confidentially.

I will acknowledge the report within a few days and aim to release a fix within 30 days, depending on severity and complexity.

## Response Timeline

This is a single-maintainer open-source project. I will do my best to:

- **Acknowledge** your report within **7 days**
- **Assess and respond** with a plan within **14 days**
- **Release a fix** within **30 days** where technically feasible

## Security Considerations

This integration stores your **EKZ portal password** and **TOTP secret** in Home Assistant's config entry storage. Please ensure:

- Your Home Assistant instance is not publicly accessible without authentication.
- You use a strong HA login and enable MFA if possible.
- You treat your HA configuration files (`config/.storage/`) as sensitive — they contain credentials in plaintext.

> **Note:** This integration only performs **read-only** access to your EKZ energy consumption data. It cannot control devices or modify your EKZ account.

## Scope

Reports are welcome for:

- Credential leakage (e.g. secrets appearing in logs)
- Dependency vulnerabilities in `aiohttp`, `beautifulsoup4`, or `pyotp`
- Authentication bypass or session handling issues in `EkzFetcher`

Out of scope:

- Vulnerabilities in Home Assistant Core itself (report to [home-assistant/core](https://github.com/home-assistant/core/security))
- Issues requiring physical access to the HA host
