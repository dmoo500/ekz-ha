# EKZ Home Assistant Integration - Clean Architecture

This document describes the integration architecture (v0.3.x).

## Architecture Overview

The integration is organized into four main layers:

```
custom_components/ekz_ha/
├── __init__.py       # Setup, service registration and teardown
├── coordinator.py    # Discovery, import orchestration and adaptive polling
├── api/              # API Layer - Communication with EKZ servers
├── data/             # Data Layer - Processing and transformation
├── statistics/       # Statistics Layer - Import logic
└── utils/            # Utility functions
```

## Layer Responsibilities

### Integration Lifecycle and Coordination

- **`__init__.py`**: Config-entry setup/unload, prediction timer registration and
  the statistics-reset service.
- **`coordinator.py`**: Installation discovery, consumption/production import
  orchestration, running-sum restoration and adaptive polling. Imports, local
  prediction extension and statistics reset share the same lock.

### API Layer (`api/`)

Handles all communication with the EKZ API.

- **`models.py`**: Type-safe dataclasses for API responses
  - `ApiValue`: Single measurement value
  - `ConsumptionData`: Complete API response with HT/NT series
  - `InstallationData`, `InstallationSelectionData`: Installation metadata

- **`session.py`**: Low-level HTTP session management
  - Login/authentication
  - Session lifecycle
  - Automatic retry on timeout

- **`client.py`**: High-level API client
  - Type-safe methods (`get_consumption_15min()`, `get_production_15min()`, etc.)
  - Wraps Session and returns typed models
  - Easy-to-use interface for consumers

### Data Layer (`data/`)

Processes raw API data into Home Assistant-compatible format.

- **`aggregator.py`**: Data aggregation logic
  - Merge HT/NT tariffs
  - Aggregate 15-min → hourly
  - Aggregate → daily

- **`validator.py`**: Data validation
  - Count slots per day
  - Identify complete days (96/92/100 slots for DST)
  - Find latest complete date

- **`transformer.py`**: Statistics transformation
  - Convert `ApiValue` → HA statistics format
  - Calculate running sums
  - Handle timestamps (UTC conversion)

### Statistics Layer (`statistics/`)

Orchestrates data import workflows.

- **`importer.py`**: Base importer class
  - Common import logic
  - Date range calculation
  - Metadata tracking

- **`consumption.py`**: Consumption-specific importer
  - Tries 15-min data first
  - Fills days without 15-min data using daily values

- **`production.py`**: Production-specific importer
  - Handles solar feed-in data
  - 15-min resolution only

- **`checkpoint.py`**: Durable import state and failure recovery
  - Source-resolution tracking and cumulative-sum offset caching
  - Checkpoint persistence/restoration and recorder replay
  - Import transactions, rollback and failure diagnostics

- **`prediction.py`**: Prediction calculations and serializable learning profiles
- **`prediction_manager.py`**: Profile persistence and recorder-based recovery,
  learning after imports, replacement of estimates with real readings and hourly
  local extension within the 14-day measurement-age limit
- **`recorder.py`**: Stable external statistic IDs, legacy entity-ID lookup,
  shared metadata, statistics writes, recorder synchronization and clearing compatibility
- **`migration.py`**: One-time, resumable migration of retained recorder history
  to external statistics, overlap verification and checkpoint/source-resolution rekeying

Existing import and prediction stores retain their formats. The external-statistics
migration has its own progress store and runs before imports resume. Historical
timestamps, states and cumulative sums are preserved, and old statistics remain
available. Users must select the new external statistics in the Energy Dashboard;
no statistics reset is required.

## Usage Example

```python
from datetime import datetime

from custom_components.ekz_ha.api.client import EkzApiClient
from custom_components.ekz_ha.statistics import ConsumptionImporter, ProductionImporter

# Initialize API client
api = EkzApiClient(username, password, totp_secret)

# Create importers
consumption_importer = ConsumptionImporter(api)
production_importer = ProductionImporter(api)

# Import consumption data
result = await consumption_importer.import_statistics(
    hass=hass,
    installation_id="779053",
    contract_start=datetime(2019, 8, 16),
    meta_entity=meta_entity,
    running_sum_offset=12345.67,
)

# Result contains:
# - statistics: List of HA statistics dicts
# - last_import: Latest import date
# - from_date, to_date: Period covered

# Write to HA database
async_add_external_statistics(hass, metadata, result["statistics"])
```

## Benefits of New Architecture

### 1. **Separation of Concerns**
Each layer has a clear responsibility:
- API layer: Network communication
- Data layer: Data processing
- Statistics layer: Business logic

### 2. **Reusability**
Common logic is shared:
- `DataAggregator` used by both consumption & production
- `BaseImporter` provides common import workflow
- No code duplication

### 3. **Type Safety**
Using dataclasses throughout:
- Catch errors at development time
- IDE autocomplete
- Self-documenting code

### 4. **Testability**
Each component can be tested independently:
```python
# Test aggregator without API
aggregator = DataAggregator()
values = [ApiValue(...), ApiValue(...)]
hourly = aggregator.aggregate_to_hourly(values)

# Test importer with mock API
mock_api = Mock(spec=EkzApiClient)
importer = ConsumptionImporter(mock_api)
```

### 5. **Maintainability**
- Focused modules organized by responsibility
- Clear naming conventions
- Comprehensive docstrings
- Easy to locate and fix bugs

## Migration Path

The old `EkzFetcher.py` is gradually being replaced:

1. ✅ API models created
2. ✅ Data processing extracted
3. ✅ Base importer created
4. 🔄 Update `__init__.py` to use new importers
5. 🔄 Update `sensor.py` to use new models
6. ✅ Remove old `EkzFetcher.py`

## Testing Strategy

Each layer should have unit tests:

```
tests/
├── api/
│   ├── test_models.py
│   ├── test_client.py
│   └── test_session.py
├── data/
│   ├── test_aggregator.py
│   ├── test_validator.py
│   └── test_transformer.py
└── statistics/
    ├── test_consumption.py
    └── test_production.py
```

## Future Enhancements

With clean architecture in place, we can easily add:

- **Caching layer**: Cache API responses
- **Rate limiting**: Prevent API overload
- **Metrics**: Track import performance
- **Additional data types**: Gas, water, etc.
- **Multiple tariff support**: Better HT/NT handling
