

# EKZ Home Assistant Integration (HACS)

This integration allows you to import and analyze EKZ electricity meter data directly in Home Assistant. It is installable via [HACS](https://hacs.xyz/) and supports automatic assignment of devices and entities per installation ID.

## Installation via HACS

1. Open HACS and select "Custom Repositories".
2. Add this GitHub repository (`https://github.com/dmoo500/ekz-ha`) as a custom repository (type: Integration).
3. Search for "EKZ" and install the integration.
4. Restart Home Assistant.
5. Add the integration via the user interface and enter your credentials (see **Configuration** below).

## Configuration

During setup, the following fields are required:

| Field | Description |
|---|---|
| **Username** | Your myEKZ login e-mail address |
| **Password** | Your myEKZ password |
| **TOTP Secret** | The secret key from your authenticator app (see below) |

### Two-Factor Authentication (2FA)

EKZ requires 2FA on login. The integration supports **authenticator app (TOTP)** based 2FA — SMS-based 2FA is **not** supported.

**Setup steps:**
1. Log in to [my.ekz.ch](https://my.ekz.ch) and open your account security settings.
2. Click "Add authenticator app" and choose a device name.
3. When the QR code is shown, also reveal the **secret key** (usually labeled "Key", "Secret" or "Schlüssel anzeigen"). It looks like `JBSWY3DPEHPK3PXP`. Copy it.
4. Scan the QR code with your authenticator app (e.g. Google Authenticator, Aegis, Authy) and confirm the registration with the generated code on the EKZ website.
5. Enter the secret key from step 3 into the **TOTP Secret** field when configuring this integration.

The integration uses the secret key to automatically generate the correct 6-digit code on every login — no manual interaction required.

> **Tip:** If you no longer have the secret key from when you set up the authenticator app, remove the existing authenticator in your EKZ account security settings and re-add it, noting down the secret key this time.

> **Note:** If your account currently uses SMS 2FA, you must **disable it first** and switch to an authenticator app. Go to your EKZ account security settings:
> [login.ekz.ch → Account Security → Signing In](https://login.ekz.ch/auth/realms/myEKZ/account/?referrer=cos-myekz-webapp&referrer_uri=https://my.ekz.ch/nutzerdaten/#/account-security/signing-in)
> Remove the SMS method and add an authenticator app there.

## How it works
- For each EKZ installation ID, a separate device is created.
- The corresponding entities (sensors, meta-entity) are assigned to the respective device.
- The integration imports consumption data step by step and maintains metadata such as contract start and last import.

## History
- Initially, an attempt was made to realize the import via the EKZ Energy Assistant. However, this required an additional add-on for login, which made setup more difficult.
- Afterwards, the original ekz-ha GitHub repository was found and a fork was created to improve the integration and adapt it to personal requirements. This repository is the result of that work.
- The integration was extended so that a separate device is created for each installation ID and all related entities refer to it.
- TOTP-based 2FA support was added after EKZ made 2FA mandatory and removed the option to disable it.

## Credits
- Based on the original repository by [stefanloerwald/ekz-ha](https://github.com/stefanloerwald/ekz-ha).
- Further developed and adapted by dmoo500.
