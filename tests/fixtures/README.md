# Test Fixtures

Dieses Verzeichnis enthält echte JSON-Antworten von der EKZ API, die als Fixtures für Integration-Tests verwendet werden.

## Datenschutz

**Wichtig:** Diese Fixture-Dateien enthalten **keine sensiblen persönlichen Daten**. Sie beinhalten nur:
- Verbrauchswerte in kWh (anonyme Zahlen)
- Zeitstempel und Daten
- Technische Status-Informationen

Sensible Daten wie Vertragsnummern, Installation-IDs oder persönliche Identifikatoren sind **nicht** in diesen Dateien enthalten. Diese werden nur in separaten API-Responses (z.B. `installation_selection_data`) zurückgegeben und sollten in Tests mit generischen Mock-Werten ersetzt werden.

### Empfohlene Test-IDs für Mocks

Falls Installation- oder Vertragsdaten in Tests benötigt werden, verwende diese generischen Werte:

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
  - 15-Minuten Verbrauchsdaten (QUARTER_HOUR)
  - Periode: 06.03.2026 - 16.03.2026
  - Enthält: seriesHt, seriesNt mit je 4028 Werten
  - Verwendet in: `tests/integration/test_consumption_import.py`

- **import_2026_03_16-2026_04_16-PK_VERB_TAG_METER.json** (9.6KB)
  - Tägliche Verbrauchsdaten (DAY)
  - Periode: 16.03.2026 - 16.04.2026
  - Enthält: seriesHt, seriesNt mit Tageswerten
  - Verwendet in: `tests/integration/test_consumption_import.py`

- **import_2026-01-31-2026-03-02.json** (884KB)
  - 15-Minuten Verbrauchsdaten
  - Periode: 31.01.2026 - 02.03.2026
  - Alternative Testdaten

- **import_2026-04-03-2026-05-15_PK_VERB_16Min.json** (893KB)
  - 15-Minuten Verbrauchsdaten
  - Periode: 03.04.2026 - 15.05.2026
  - Alternative Testdaten

## Production Fixtures

- **import_production_2026-08-03_WIRK_NEG_15MIN.json**
  - 15-Minuten Produktionsdaten (Rücklieferung/Solar)
  - Periode: 03.08.2026 (1 Tag)
  - Enthält: seriesNt mit 96 Werten (4 pro Stunde)
  - Quelle: Extrahiert aus GitHub Issue #18 Debug-Logs
  - Verwendet in: Geplant für Production-Integration-Tests

## Hinweise

- Diese Dateien enthalten echte API-Responses im JSON-Format
- Sensible Daten (Installation-IDs, Vertragsnummern) sind in den Tests gemockt
- Die Zeitstempel verwenden das EKZ-Format: `YYYYMMDDHHmmss`
- DST-Übergänge sind in den Daten enthalten (92/96/100 Slots pro Tag)
