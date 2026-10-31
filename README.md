

# EKZ Home Assistant Integration (HACS)

[![hacs_badge](https://img.shields.io/badge/HACS-Custom-orange.svg)](https://github.com/hacs/integration)
[![GitHub release](https://img.shields.io/github/v/release/dmoo500/ekz-ha)](https://github.com/dmoo500/ekz-ha/releases)
[![License](https://img.shields.io/github/license/dmoo500/ekz-ha)](LICENSE)
[![GitHub issues](https://img.shields.io/github/issues/dmoo500/ekz-ha)](https://github.com/dmoo500/ekz-ha/issues)
[![GitHub stars](https://img.shields.io/github/stars/dmoo500/ekz-ha)](https://github.com/dmoo500/ekz-ha/stargazers)
[![GitHub watchers](https://img.shields.io/github/watchers/dmoo500/ekz-ha)](https://github.com/dmoo500/ekz-ha/watchers)
[![GitHub last commit](https://img.shields.io/github/last-commit/dmoo500/ekz-ha)](https://github.com/dmoo500/ekz-ha/commits)
[![Maintenance](https://img.shields.io/badge/Maintained%3F-yes-green.svg)](https://github.com/dmoo500/ekz-ha)
[![Python 3.12+](https://img.shields.io/badge/Python-3.12%2B-3776AB?logo=python&logoColor=white)](https://www.python.org/)

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
4. Enter the secret key into the **TOTP Secret** field when configuring this integration.
5. Home Assistant will show a current 6-digit confirmation code in the next step. If you are registering a new authenticator, enter that code and the device name on the EKZ website, save the registration there, then return to Home Assistant and submit the confirmation step. If the authenticator is already registered, simply submit the confirmation step.
6. The integration uses the secret key to generate a fresh 6-digit code for each login. You can also scan the QR code into your authenticator app as a backup.

> **Tip:** If you no longer have the secret key from when you set up the authenticator app, remove the existing authenticator in your EKZ account security settings and re-add it, noting down the secret key this time.

> **Note:** If your account currently uses SMS 2FA, you must **disable it first** and switch to an authenticator app. Go to your EKZ account security settings:
> [login.ekz.ch → Account Security → Signing In](https://login.ekz.ch/auth/realms/myEKZ/account/?referrer=cos-myekz-webapp&referrer_uri=https://my.ekz.ch/nutzerdaten/#/account-security/signing-in)
> Remove the SMS method and add an authenticator app there.

## How it works
- For each EKZ installation ID, a separate device is created.
- The corresponding entities (sensors, meta-entity) are assigned to the respective device.
- The integration imports consumption data step by step and maintains metadata such as contract start and last import.

### Historical data backfill & chunk imports

On first setup (or after a re-installation), the integration starts looking for historical data at your contract start date. If the initial 30-day window has no readings, it advances past the successfully checked empty period and progressively increases the next search window until data is found or the present is reached. Each API request covers at most **360 days**. The search position is saved across restarts; failed requests do not advance it. The actual contract start date remains unchanged. Once data is available, it imports **30-day chunks** directly into the Home Assistant statistics database. There is no fixed ten-year history cutoff. After reaching the present, regular updates recheck the recent period for delayed readings. Resetting statistics also resets the historical search position.

If slicing or a bounded refetch moves the import start forward, existing recorder or checkpoint contributions in the skipped interval remain included in the cumulative sum. This applies to both consumption and production.

The import progress is exposed by each installation's **Last Import** meta sensor, which stores the date of the last successfully imported data. After a Home Assistant restart, the import automatically resumes from where it left off. The last checkpoint's statistics are replayed into the recorder before either consumption or production imports resume, so queued readings are recovered even if Home Assistant stopped before writing them to its database.

#### Import diagnostics (0.2.1)

Each consumption and production installation has its own **Import status** sensor,
with names and states translated into English and German:

| State | Meaning |
|---|---|
| `waiting` | No import attempt has run yet. |
| `searching` | The initial historical search found no readings and is extending its search window. |
| `importing` | The integration is working through historical periods. |
| `current` | The requested window includes today (exclusive end is after today); EKZ readings may still arrive with a delay. |
| `failed` | An import or coordinator update failed. Inspect the diagnostic attributes and logs. |

The attributes `requested_from` and `requested_to` show the most recently attempted
API window (start included, end excluded), including a narrower follow-up request
after a successful historical search. `last_attempt` records when that request
started. `last_result` distinguishes `imported`, `no_data`, and `failed`.
`no_data` means EKZ returned no importable readings, even if previously imported
statistics were preserved during reconciliation.
`error_type` identifies the exception class for an import failure without exposing
account details. A coordinator-level failure also exposes `coordinator_error_type`.
`next_update` shows the planned update after a successful coordinator cycle.

**Last Import** now shows the latest successfully imported statistic timestamp,
not the contract start or an empty search cursor. It is `unknown` before any
successful import and retains its previous value when an update fails. Both it
and the status sensor remain available for troubleshooting during failed updates.
Existing checkpoints and recorder history are used to recover the timestamp on
upgrade; no statistics reset is needed for these sensors.
Saved diagnostics for all installations are restored before imports begin, even
if an earlier installation subsequently fails. Failed attempts also save their
diagnostic details without advancing the last safe import checkpoint. If storage
itself fails, the integration logs the error and retains the diagnostics in memory.
After a successful installation discovery, the integration caches the known
installations locally. On a later restart with EKZ unreachable, it restores their
diagnostic sensors and saved checkpoints, shows the startup failure, and retries
discovery before importing again. This requires at least one successful startup
with 0.2.1; a first setup without cached installations still waits for EKZ.
Authentication failures continue to use Home Assistant's normal reauthentication
flow rather than the offline fallback.
Rejected credentials or TOTP codes prompt for replacement credentials and TOTP
confirmation in Home Assistant, then reload the existing integration without
resetting its statistics. Maintenance pages and server outages do not start
reauthentication. If offline recovery discovers new or replaced installations,
the entry reloads automatically to register their sensors before imports resume.
`imported` means at least one returned reading was accepted by reconciliation;
partial readings discarded in favor of stored daily/hourly statistics remain
`no_data`. An expanding historical search stays `searching` even when stored
rows are replayed.
Diagnostic sensors publish the attempted window as each request starts, including
bounded follow-up requests, and publish each completed or failed chunk without
waiting for other installations. Retries show their local progress even while
the coordinator still retains the previous cycle's failure.

Sensor registration does not wait for historical imports. Once the sensor
platform is loaded, the first import runs as a background task tied to the
integration entry; unloading the entry cancels it. Initial installation discovery
still runs during setup (or uses the offline fallback described above).

If the result is `no_data` while searching, allow the next update to search a
larger period. For `failed`, enable debug logging, reload the integration once,
and include the attempted dates and exception class when reporting an issue.
Do not reset statistics solely because a historical period is empty.

Consumption is fetched at 15-minute resolution where available. If a day has no quarter-hour readings, the integration uses EKZ's daily total for that day instead. Within the most recent 30 days, daily-fallback and incomplete days are revisited so corrected daily totals can be imported. For consumption days already represented by a daily fallback, the latest daily total is fetched even when some quarter-hour readings are available. Only a complete set of quarter-hour readings replaces a daily fallback; partial readings retain the latest available daily total. This reconciliation also applies to production imports.

When EKZ supplies separate high- and low-tariff series (HT/NT), each timestamp must have either a valid reading or an explicit inactive zero (`NOT_AVAILABLE` with value `0`) in each supplied tariff. This supports alternating HT/NT periods without discarding the active tariff's measurement. `MISSING`, absent timestamps and nonzero `NOT_AVAILABLE` entries do not count as coverage; incomplete tariff data cannot replace a daily fallback. Inactive entries never create measurements on their own. This applies to consumption and production; single-series and TOTAL responses remain supported.

#### Adaptive polling interval

To speed up the backfill without overwhelming the EKZ API, the integration automatically adjusts the polling interval:

| State | Polling interval |
|---|---|
| Initial/default interval | **20 minutes** |
| Backlog present (historical data not yet fully imported) | **5 minutes** |
| Caught up (import is up to date) | **4 hours** |

After the first update determines the import status, the integration switches to the catch-up or normal interval automatically. These intervals are defined in [`const.py`](custom_components/ekz_ha/const.py).

#### Energy Dashboard

After the first chunk is imported, you can add the external statistic to the Energy Dashboard:
1. Go to **Energy Dashboard → ⋮ → Energy configuration → Electricity grid → Add consumption sensor**
2. Select **EKZ \<installationId\> Electricity consumption** (`ekz_ha:electricity_consumption_<installationId>`) for "Energy imported from grid"
3. (Optional) Select **EKZ \<installationId\> Electricity production** (`ekz_ha:electricity_production_<installationId>`) for "Energy exported to grid"
4. Historical data already imported will appear immediately; remaining chunks will appear as each update cycle completes.

Starting with 0.3.0, consumption, production and prediction use stable external statistics, not sensor entities with an `unknown` state. Renaming diagnostic sensors does not affect these IDs. All diagnostic and import-status sensors remain available.

Open **Energy** in the sidebar, then **⋮ → Energy configuration** (direct URL:
`/config/energy`). This is the Energy Dashboard's configuration, not a separate
Energy item in the general Settings menu.

On upgrade, existing hourly statistics are copied locally using Home Assistant's supported external-statistics API, preserving timestamps, states and cumulative sums. Renamed sensor IDs, import checkpoints and prediction learning are retained. The first import is scheduled after Home Assistant has started so the recorder can process its queue. Migration runs before new imports, in bounded batches, and resumes after a restart. Fresh installations without legacy statistics skip the historical scan. Conflicting histories, including incompatible cumulative baselines where separate series join, stop the affected import with an explicit error rather than overwriting data. Ambiguous joins across gaps are not automatically rebased.

**Upgrade action:** A Repairs notice lists the new statistics. Replace the old EKZ selections in **Energy Dashboard → ⋮ → Energy configuration**, including optional production and prediction entries. Dashboard selections are not changed automatically. The old energy sensor entities are removed, but their historical statistics are retained. Those old statistics may still show warnings in **Developer Tools → Statistics**; the new external selections avoid the missing-current-state warning. Do not delete or reset statistics to perform this upgrade.

> **Spook / orphaned-statistics cleanup:** The old consumption, production and prediction sensor entities are removed during upgrade, before the history migration necessarily finishes. Warnings about these missing entities are expected. Do not delete their statistics through Spook, Developer Tools or other cleanup tools while migration is pending or has failed: those statistics are still needed as the migration source. Before considering any cleanup, confirm successful migration, check the historical data under the new `ekz_ha:...` statistics, update all Energy Dashboard selections and create a backup. Removing old entity references from the dashboard is not the same as deleting their historical data.

> **Note:** The Energy Dashboard shows the current period by default. Use the `<` arrow to navigate back to earlier months to verify historical data.

### Prediction statistics (gap filling)

EKZ delivers consumption data with a delay of approximately 7–10 days. To fill this gap in the Energy Dashboard, the integration provides **prediction statistics**:

`ekz_ha:electricity_consumption_<installationId>_prediction`

**How it works:**
- While importing historical data, the integration accumulates **hourly consumption averages** per UTC month+hour bucket from the raw 15-minute slots.
- Historical contributions remain in these aggregate buckets. Individual slots are retained only for the latest 32 days of imported data to handle repeated readings and corrections within the 30-day import lookback. Older slots are frozen in the aggregates and ignored if returned again, keeping prediction memory bounded.
- The learned profile and recent correction bookkeeping are saved across Home Assistant restarts. On upgrade, if no saved profile exists, the integration rebuilds it from up to 400 days of real hourly consumption statistics. Daily-total days, gaps and negative consumption differences are excluded. Later raw imports replace overlapping seeded hours rather than counting them twice.
- If loading or rebuilding the profile temporarily fails, consumption imports continue, but learning from their raw readings is deferred until recovery succeeds. Incomplete profiles are not saved over existing history; profile recovery is retried on subsequent updates.
- Once the historical import is complete (catch-up mode ends), each update cycle generates predicted hourly values from the last real data point up to the current time.
- Between EKZ updates, predictions extend locally at one minute past each hour, without additional EKZ API calls. Extensions pause during imports, catch-up and statistics resets, and stop if the latest real measurement is more than **14 days** old. Fresh predictions do not extend that limit.
- Resetting statistics also clears the saved learning profile. Existing historical measurements do not need to be deleted to recover predictions after a normal restart.
- The predictions are written as HA statistics — the Energy Dashboard will show them as an estimate for the recent gap.

**Adding prediction statistics to the Energy Dashboard:**
1. Go to **Energy Dashboard → ⋮ → Energy configuration → Electricity grid → Add consumption sensor**
2. Select **EKZ \<installationId\> Electricity consumption prediction** (`ekz_ha:electricity_consumption_<installationId>_prediction`)
3. Predictions are generated once catch-up completes. Existing prediction history is preserved on upgrade.

> **Note:** Predictions are based on historical averages per hour and month. They become more accurate the more historical data has been imported. The predictions are automatically overwritten by real data as EKZ delivers it.

## Solar production data

If you have one or more EKZ solar installations (feed-in / Einspeisung), the integration automatically detects them and imports your production history into Home Assistant long-term statistics. Accounts with production contracts but no consumption contract are supported as well; in that case, only production statistics and diagnostic entities are created.

Add production data to the Energy Dashboard:
1. Go to **Energy Dashboard → ⋮ → Energy configuration → Solar panels → Add solar production**
2. Select **EKZ \<installationId\> Electricity production** (`ekz_ha:electricity_production_<installationId>`)

Production diagnostic entities are grouped under a device for the relevant EKZ installation.

A failed production import or checkpoint replay is logged and marked `failed` for that installation, but does not stop imports for other production installations or make successfully updated consumption unavailable. Its safe import position is retained for the next retry. A successful request with no new readings also counts as a completed update. If every attempted import fails on a production-only account, the coordinator reports an update failure. Authentication failures still trigger Home Assistant's reauthentication flow immediately.

> **Note:** As with consumption, the initial import works through your full history in 30-day chunks. Navigate back in the Energy Dashboard to verify historical production data.

---

## Resetting statistics (`ekz_ha.reset_statistics`)

If your statistics are incorrect (e.g. after a timezone fix or a reinstall), you can trigger a full re-import using the built-in reset service.

### What it does
1. Deletes all EKZ statistics from the Home Assistant database (consumption, predictions, production), including retained pre-0.3.0 history
   and verifies completion before clearing import state or starting a new import
2. Resets the in-memory import state
3. Immediately starts a fresh re-import from your contract start date

### How to use
1. Go to **Developer Tools → Actions**
2. Search for `ekz_ha.reset_statistics`
3. Select your entry (if you have multiple EKZ config entries) and press **Perform action**

> **Warning:** This permanently deletes all previously imported EKZ statistics. The re-import will start automatically but takes multiple poll cycles to complete (depending on how much history needs to be re-fetched).

## Reporting a problem

Use the [bug-report form](https://github.com/dmoo500/ekz-ha/issues/new?template=bug_report.yml).
Include your exact EKZ integration version (including any beta suffix), Home Assistant version,
steps to reproduce, import-status attributes, actual selected statistic IDs and any warning text.

To collect debug logs, open **Settings → Devices & services → Integrations → EKZ**,
select **Enable debug logging** in the three-dot menu, reproduce the problem, then select
**Disable debug logging** to download the captured log file. For an import failure, reload
the integration once and wait for an update attempt. The issue form also explains how to
enable logging through `configuration.yaml` if the integration cannot load.

Inspect and sanitize logs before attaching them: remove passwords, TOTP secrets/codes,
cookies, tokens, e-mail addresses and other personal data. Debug logs may contain API
responses and energy readings. Preserve timestamps and tracebacks, and replace installation
IDs consistently when masking them. Do not reset or delete statistics just to collect logs.

---

## History
- Initially, an attempt was made to realize the import via the EKZ Energy Assistant. However, this required an additional add-on for login, which made setup more difficult.
- Afterwards, the original ekz-ha GitHub repository was found and a fork was created to improve the integration and adapt it to personal requirements. This repository is the result of that work.
- The integration was extended so that a separate device is created for each installation ID and all related entities refer to it.
- TOTP-based 2FA support was added after EKZ made 2FA mandatory and removed the option to disable it.

## Credits
- Originally started by [stefanloerwald](https://github.com/stefanloerwald/ekz-ha).
- Forked and actively maintained by [dmoo500](https://github.com/dmoo500/ekz-ha), who implemented all current features including device/entity management, statistics import, TOTP-based 2FA support, and HA 2025 compatibility.
