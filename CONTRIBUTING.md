# Contributing to EKZ Home Assistant Integration

Thank you for your interest in this project! These guidelines will help you contribute.

## 🏗️ Architecture

See [ARCHITECTURE.md](custom_components/ekz_ha/ARCHITECTURE.md) for details about the code structure.

## 🔧 Development Setup

```bash
# Clone repository
git clone https://github.com/dmoo500/ekz-ha.git
cd ekz-ha

# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate  # Linux/Mac
# or: .venv\Scripts\activate  # Windows

# Install dependencies
pip install -e ".[dev]"

# Install pre-commit hooks
pip install pre-commit
pre-commit install
```

## 🪝 Pre-commit Hooks

This project uses pre-commit hooks to ensure code quality. The hooks run automatically on `git commit`:

- **ruff** - Fast Python linter and formatter
- **mypy** - Static type checking
- **pytest** - Run tests before push
- **JSON/TOML validation** - Validate configuration files
- **Security checks** - Detect private keys, merge conflicts
- **File formatting** - Fix trailing whitespace, end-of-file

### Manual Hook Execution

```bash
# Run all hooks on all files
pre-commit run --all-files

# Run specific hook
pre-commit run ruff-check --all-files
pre-commit run mypy --all-files

# Run hooks before push
pre-commit run --hook-stage pre-push --all-files
```

### Skipping Hooks

Only skip hooks when absolutely necessary:

```bash
# Skip all hooks (not recommended)
git commit --no-verify -m "message"

# Skip specific hook
SKIP=mypy git commit -m "message"
```

## 🧪 Tests

```bash
# Run all tests
pytest

# With coverage
pytest --cov=custom_components.ekz_ha --cov-report=html

# Run specific test
pytest tests/api/test_models.py::TestApiValue::test_from_dict_standard_format

# Run integration tests
pytest tests/integration/ -v
```

## 📝 Code Quality

### Linting & Formatting

```bash
# Format code
ruff format custom_components/ tests/

# Linting
ruff check custom_components/ tests/

# Auto-fix
ruff check --fix custom_components/ tests/

# Type checking
mypy custom_components/ekz_ha/
```

### Pre-Commit Checklist

Before each commit (automated via pre-commit hooks):

- [ ] Tests pass: `pytest`
- [ ] Linting is clean: `ruff check custom_components/ tests/`
- [ ] Code is formatted: `ruff format custom_components/ tests/`
- [ ] Type checking passes: `mypy custom_components/ekz_ha/`
- [ ] No new errors in Home Assistant logs

## 🌿 Branching Strategy

### Branch Names

- `feat/description` - New features
- `fix/issue-number-description` - Bug fixes
- `refactor/description` - Code refactoring
- `docs/description` - Documentation

### Examples

```bash
git checkout -b feat/solar-forecast
git checkout -b fix/issue-23-connection-timeout
git checkout -b refactor/clean-architecture
```

## 📤 Pull Requests

### Creating a PR

1. **Create branch from `main`**
   ```bash
   git checkout main
   git pull origin main
   git checkout -b feat/my-feature
   ```

2. **Commit changes**
   ```bash
   # Add individual files (NOT git add -A)
   git add custom_components/ekz_ha/api/client.py
   git add tests/api/test_client.py
   git commit -m "feat: Add retry logic to API client"
   ```

3. **Push and create PR**
   ```bash
   git push -u origin feat/my-feature
   ```

### Commit Messages

Format: `<type>: <description>`

**Types:**
- `feat:` - New feature
- `fix:` - Bug fix
- `refactor:` - Code refactoring
- `docs:` - Documentation
- `test:` - Tests
- `chore:` - Build, dependencies, etc.

**Examples:**
```
feat: Add solar production forecasting
fix: Resolve session timeout during automatic sync
refactor: Extract aggregation logic to separate module
docs: Update installation instructions
test: Add tests for DST handling
chore: Update dependencies to latest versions
```

### PR Template

The PR template loads automatically. Fill out all sections:

- Description of changes
- Type of change (feature, bugfix, etc.)
- Related issues (e.g., "Fixes #18")
- Test status
- Checklist

## 🏷️ Versioning & Releases

### Version Format

**IMPORTANT:** We use **NO** "v" prefix before version numbers!

✅ **Correct:**
```bash
git tag 0.2.0-alpha.1
git tag 0.2.0
git tag 1.0.0
```

❌ **Wrong:**
```bash
git tag v0.2.0-alpha.1  # DO NOT USE
git tag v0.2.0          # DO NOT USE
```

### Semantic Versioning

Format: `MAJOR.MINOR.PATCH[-PRERELEASE]`

- **MAJOR** - Breaking changes
- **MINOR** - New features (backwards compatible)
- **PATCH** - Bug fixes
- **PRERELEASE** - alpha, beta, rc

**Examples:**
- `0.1.12` - Patch release
- `0.2.0` - Minor release with new features
- `0.2.0-alpha.1` - Alpha pre-release
- `0.2.0-beta.1` - Beta pre-release
- `1.0.0` - Major release

### Creating a Release

```bash
# 1. Update version in manifest.json
# 2. Commit
git add custom_components/ekz_ha/manifest.json
git commit -m "chore: Bump version to 0.2.0"

# 3. Create tag (WITHOUT "v"!)
git tag 0.2.0

# 4. Push with tags
git push origin main
git push origin 0.2.0
```

The GitHub workflow automatically creates a release with changelog.

## 🐛 Bug Reports

Use the [Bug Report Template](.github/ISSUE_TEMPLATE/bug_report.yml).

Important information:
- EKZ Integration version
- Home Assistant version
- Logs from Home Assistant
- Steps to reproduce

## 💡 Feature Requests

Use the [Feature Request Template](.github/ISSUE_TEMPLATE/feature_request.yml).

Describe:
- Use case / problem
- Proposed solution
- Alternative approaches

## 📚 Code Style

### Python

- Follow PEP 8
- Use type hints (Python 3.12+ syntax: `str | None`)
- Use dataclasses for data structures
- Prefer `async`/`await` over callbacks
- Maximum line length: 100 characters

### Imports

```python
# Standard library
import logging
from datetime import datetime, timedelta

# Third-party
from homeassistant.core import HomeAssistant

# Local
from .api.client import EkzApiClient
from .const import DOMAIN
```

### Docstrings

```python
def calculate_average(values: list[float]) -> float:
    """Calculate the arithmetic mean of a list of values.

    Args:
        values: List of numeric values

    Returns:
        The arithmetic mean

    Raises:
        ValueError: If the list is empty
    """
    if not values:
        raise ValueError("Cannot calculate average of empty list")
    return sum(values) / len(values)
```

## 🧪 Testing Guidelines

### Test Structure

```python
"""Tests for data aggregation."""

import pytest
from custom_components.ekz_ha.data import DataAggregator


class TestDataAggregator:
    """Test DataAggregator class."""

    @pytest.fixture
    def aggregator(self):
        """Create aggregator instance."""
        return DataAggregator()

    def test_merge_tariffs(self, aggregator):
        """Test merging HT and NT tariffs."""
        # Arrange
        values = [...]

        # Act
        result = aggregator.merge_tariffs(values)

        # Assert
        assert len(result) == 2
        assert result[0].value == 3.0
```

### Test Coverage

- Aim for >80% code coverage
- Write unit tests for all business logic
- Add integration tests for end-to-end workflows
- Test edge cases and error conditions

## 🔍 Debugging

### Enable Debug Logging

Add to `configuration.yaml`:

```yaml
logger:
  default: info
  logs:
    custom_components.ekz_ha: debug
```

### VSCode Debug Configuration

`.vscode/launch.json`:

```json
{
  "version": "0.2.0",
  "configurations": [
    {
      "name": "Python: Current File",
      "type": "debugpy",
      "request": "launch",
      "program": "${file}",
      "console": "integratedTerminal",
      "justMyCode": false
    }
  ]
}
```

## 📖 Documentation

- Update README.md for user-facing changes
- Update ARCHITECTURE.md for structural changes
- Add docstrings to all public APIs
- Include code examples in docstrings

## ❓ Questions?

- Open a [Discussion](https://github.com/dmoo500/ekz-ha/discussions)
- Check existing [Issues](https://github.com/dmoo500/ekz-ha/issues)
- Review [Architecture Documentation](custom_components/ekz_ha/ARCHITECTURE.md)

---

Thank you for contributing! 🎉
