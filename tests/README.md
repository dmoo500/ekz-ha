# Testing

This project uses pytest for testing.

## Setup

Install development dependencies:

```bash
pip install -e ".[dev]"
```

## Running Tests

Run all tests:

```bash
pytest
```

Run with coverage:

```bash
pytest --cov=custom_components.ekz_ha --cov-report=html
```

Run specific test file:

```bash
pytest tests/data/test_aggregator.py
```

Run specific test:

```bash
pytest tests/data/test_aggregator.py::TestDataAggregator::test_merge_tariffs_single_timestamp
```

## Test Structure

```
tests/
├── api/              # API layer tests
│   └── test_models.py
├── data/             # Data processing tests
│   ├── test_aggregator.py
│   ├── test_validator.py
│   └── test_transformer.py
├── statistics/       # Import logic tests
│   ├── test_consumption.py
│   └── test_production.py
└── conftest.py       # Shared fixtures
```

## Writing Tests

### Test Organization

- One test file per module
- Use descriptive test class names (`TestDataAggregator`)
- Use descriptive test function names (`test_merge_tariffs_single_timestamp`)

### Fixtures

Common test fixtures are defined in `conftest.py`:

```python
def test_my_feature(sample_api_values):
    # sample_api_values is automatically provided by pytest
    result = process(sample_api_values)
    assert result is not None
```

### Async Tests

For async tests, use the `@pytest.mark.asyncio` decorator:

```python
import pytest

@pytest.mark.asyncio
async def test_async_function():
    result = await some_async_function()
    assert result is not None
```

### Mocking

Use `unittest.mock` or `pytest-mock` for mocking:

```python
from unittest.mock import Mock, AsyncMock

def test_with_mock():
    mock_api = Mock(spec=EkzApiClient)
    mock_api.get_consumption_15min = AsyncMock(return_value=ConsumptionData())
    
    importer = ConsumptionImporter(mock_api)
    # Test importer behavior
```

## Coverage Goals

- Aim for >80% code coverage
- All new features must include tests
- All bug fixes should include regression tests

## Continuous Integration

Tests are automatically run on:
- Every push to a branch
- Every pull request
- Before merging to main

## Home Assistant Testing

For integration testing with Home Assistant:

```bash
pytest --homeassistant
```

This requires `pytest-homeassistant-custom-component` to be installed.
