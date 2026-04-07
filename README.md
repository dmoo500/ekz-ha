

# EKZ Home Assistant Integration (HACS)

[![hacs_badge](https://img.shields.io/badge/HACS-Custom-orange.svg)](https://github.com/hacs/integration)
[![GitHub release](https://img.shields.io/github/v/release/dmoo500/ekz-ha)](https://github.com/dmoo500/ekz-ha/releases)
[![License](https://img.shields.io/github/license/dmoo500/ekz-ha)](LICENSE)
[![GitHub issues](https://img.shields.io/github/issues/dmoo500/ekz-ha)](https://github.com/dmoo500/ekz-ha/issues)
[![GitHub stars](https://img.shields.io/github/stars/dmoo500/ekz-ha)](https://github.com/dmoo500/ekz-ha/stargazers)
[![GitHub watchers](https://img.shields.io/github/watchers/dmoo500/ekz-ha)](https://github.com/dmoo500/ekz-ha/watchers)
[![GitHub last commit](https://img.shields.io/github/last-commit/dmoo500/ekz-ha)](https://github.com/dmoo500/ekz-ha/commits)
[![Maintenance](https://img.shields.io/badge/Maintained%3F-yes-green.svg)](https://github.com/dmoo500/ekz-ha)
[![Python](https://img.shields.io/badge/Python-3776AB?logo=python&logoColor=white)](https://www.python.org/)

This integration allows you to import and analyze EKZ electricity meter data directly in Home Assistant. It is installable via [HACS](https://hacs.xyz/) and supports automatic assignment of devices and entities per installation ID.

## Installation via HACS

[![Open your Home Assistant instance and open a repository inside the Home Assistant Community Store.](https://my.home-assistant.io/badges/hacs_repository.svg)](https://my.home-assistant.io/redirect/hacs_repository/?owner=dmoo500&repository=ekz-ha&category=integration)

1. Open HACS and select "Custom Repositories".
2. Add this GitHub repository (`https://github.com/dmoo500/ekz-ha`) as a custom repository (type: Integration).
3. Search for "EKZ" and install the integration.
4. Restart Home Assistant.
5. Add the integration via the user interface and enter your credentials (see **Configuration** below).

## Configuration

During setup, the following fields are required or optional:

| Field | Required | Description |
|---|---|---|
| **Username** | ✅ | Your myEKZ login e-mail address |
| **Password** | ✅ | Your myEKZ password |
| **TOTP Secret** | ✅ | The secret key from your authenticator app (see below) |
| **Device Name** | ☑️ optional | The name of the authenticator device as shown in your EKZ account (e.g. `Home Assistant`). Only needed if you have **multiple** authenticator devices registered — used to select the correct one during login. Leave empty to use the default/pre-selected device. |

### Two-Factor Authentication (2FA)

EKZ requires 2FA on login. The integration supports **authenticator app (TOTP)** based 2FA — SMS-based 2FA is **not** supported.

**Setup steps:**
1. Log in to [my.ekz.ch](https://my.ekz.ch) and open your account security settings.
2. Click "Add authenticator app" and choose a device name (e.g. `Home Assistant`). **Note this name** — you may enter it as **Device Name** in the integration setup if you have multiple authenticator devices registered.
3. When the QR code is shown, also reveal the **secret key** (usually labeled "Key", "Secret" or "Schlüssel anzeigen"). It looks like `JBSWY3DPEHPK3PXP`. Copy it.
4. Enter the secret key into the **TOTP Secret** field when configuring this integration and save.
   - If you have **not yet confirmed** the authenticator on the EKZ website, the integration will automatically submit the confirmation code to EKZ during setup — completing the registration transparently.
   - Afterwards, you can also scan the QR code into your authenticator app as a manual backup.
5. The integration will use the secret key to generate the correct 6-digit code on every login automatically.

> **Tip:** If you no longer have the secret key from when you set up the authenticator app, remove the existing authenticator in your EKZ account security settings and re-add it, noting down the secret key this time.

> **Note:** If your account currently uses SMS 2FA, you must **disable it first** and switch to an authenticator app. Go to your EKZ account security settings:
> [login.ekz.ch → Account Security → Signing In](https://login.ekz.ch/auth/realms/myEKZ/account/?referrer=cos-myekz-webapp&referrer_uri=https://my.ekz.ch/nutzerdaten/#/account-security/signing-in)
> Remove the SMS method and add an authenticator app there.

## How it works
- For each EKZ installation ID, a separate device is created.
- The corresponding entities (sensors, meta-entity) are assigned to the respective device.
- The integration imports consumption data step by step and maintains metadata such as contract start and last import.

### Historical data backfill & chunk imports

On first setup (or after a re-installation), the integration needs to import all historical consumption data starting from your contract start date. Because the EKZ API returns data in limited windows, the integration fetches **one 30-day chunk per update cycle** and writes it directly into the Home Assistant statistics database.

The import progress is tracked by the **meta entity** (`sensor.ekz_<installationId>_letzter_import`), which stores the date of the last successfully imported chunk. After a Home Assistant restart, the import automatically resumes from where it left off.

#### Adaptive polling interval

To speed up the backfill without overwhelming the EKZ API, the integration automatically adjusts the polling interval:

| State | Polling interval |
|---|---|
| Backlog present (historical data not yet fully imported) | **5 minutes** |
| Caught up (import is up to date) | **20 minutes** |

Once all historical data is imported, the polling interval switches back to normal automatically. Both intervals can be changed in [`const.py`](custom_components/ekz_ha/const.py).

#### Energy Dashboard

After the first chunk is imported, you can add the sensor to the Energy Dashboard:
1. Go to **Settings → Energy → Electricity grid → Add consumption sensor**
2. Select `sensor.electricity_consumption_ekz_<installationId>` for "Energy imported from grid"
3. (Optional) Select `sensor.electricity_production_ekz_<installationId>` for "Energy exported to grid"
4. Historical data already imported will appear immediately; remaining chunks will appear as each update cycle completes.

> **Note:** The Energy Dashboard shows the current period by default. Use the `<` arrow to navigate back to earlier months to verify historical data.

## Solar production data

If you have one or more EKZ solar installations (feed-in / Einspeisung), the integration automatically detects them and imports your production history into Home Assistant long-term statistics.

Production data appears in the Energy Dashboard alongside your consumption data:
1. Go to **Settings → Energy → Solar panels → Add solar production**
2. Select `sensor.electricity_production_ekz_<installationId>`

Production entities are grouped under the same device as the corresponding consumption installation.

> **Note:** As with consumption, the initial import works through your full history in 30-day chunks. Navigate back in the Energy Dashboard to verify historical production data.

---

## Resetting statistics (`ekz_ha.reset_statistics`)

If your statistics are incorrect (e.g. after a timezone fix or a reinstall), you can trigger a full re-import using the built-in reset service.

### What it does
1. Deletes all EKZ statistics from the Home Assistant database (consumption, predictions, production)
2. Resets the in-memory import state
3. Immediately starts a fresh re-import from your contract start date

### How to use
1. Go to **Developer Tools → Actions**
2. Search for `ekz_ha.reset_statistics`
3. Select your entry (if you have multiple EKZ config entries) and press **Perform action**

> **Warning:** This permanently deletes all previously imported EKZ statistics. The re-import will start automatically but takes multiple poll cycles to complete (depending on how much history needs to be re-fetched).

---

## History
- Initially, an attempt was made to realize the import via the EKZ Energy Assistant. However, this required an additional add-on for login, which made setup more difficult.
- Afterwards, the original ekz-ha GitHub repository was found and a fork was created to improve the integration and adapt it to personal requirements. This repository is the result of that work.
- The integration was extended so that a separate device is created for each installation ID and all related entities refer to it.
- TOTP-based 2FA support was added after EKZ made 2FA mandatory and removed the option to disable it.

## Credits
- Originally started by [stefanloerwald](https://github.com/stefanloerwald/ekz-ha).
- Forked and actively maintained by [dmoo500](https://github.com/dmoo500/ekz-ha), who implemented all current features including device/entity management, statistics import, TOTP-based 2FA support, and HA 2025 compatibility.
