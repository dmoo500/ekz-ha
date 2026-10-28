# Test Fixtures

This directory contains real JSON responses from the EKZ API used as fixtures for integration tests.

## Privacy

**Important:** These fixture files contain **no sensitive personal data**. They include only:
- Consumption values in kWh (anonymous numbers)
- Timestamps and dates
- Technical status information

Sensitive data such as contract numbers, installation IDs, or personal identifiers are **not** included in these files. These are only returned in separate API responses (e.g., `installation_selection_data`) and should be replaced with generic mock values in tests.

### Recommended Test IDs for Mocks

If installation or contract data is needed in tests, use these generic values:

```python
MOCK_INSTALLATION_ID = "TEST_INST_001"
MOCK_CONTRACT_DATA = {
    "gpart": "9999999",
    "vkonto": "8888888",
    "vertrag": "7777777",
    "anlage": "TEST_INST_001",
    "vstelle": "6666666",
}
```

## Consumption Fixtures

- **import_2026-03-06-2026-03-16-PK_VERB_15MIN.json** (1.2MB)
  - 15-minute consumption data (QUARTER_HOUR)
  - Period: 2026-03-06 to 2026-03-16
  - Contains: seriesHt, seriesNt with 4028 values each
  - Used in: `tests/integration/test_consumption_import.py`

- **import_2026_03_16-2026_04_16-PK_VERB_TAG_METER.json** (9.6KB)
  - Daily consumption data (DAY)
  - Period: 2026-03-16 to 2026-04-16
  - Contains: seriesHt, seriesNt with daily values
  - Used in: `tests/integration/test_consumption_import.py`

- **import_2026-01-31-2026-03-02.json** (884KB)
  - 15-minute consumption data
  - Period: 2026-01-31 to 2026-03-02
  - Alternative test data

- **import_2026-04-03-2026-05-15_PK_VERB_16Min.json** (893KB)
  - 15-minute consumption data
  - Period: 2026-04-03 to 2026-05-15
  - Alternative test data

## Production Fixtures

- **import_production_2026-08-03_WIRK_NEG_15MIN.json**
  - 15-minute production data (feed-in/solar)
  - Period: 2026-08-03 (1 day)
  - Contains: seriesNt with 96 values (4 per hour)
  - Source: Extracted from GitHub Issue #18 debug logs
  - Used in: Planned for production integration tests

## Notes

- These files contain real API responses in JSON format
- Sensitive data (installation IDs, contract numbers) are mocked in tests
- Timestamps use the EKZ format: `YYYYMMDDHHmmss`
- DST transitions are included in the data (92/96/100 slots per day)
