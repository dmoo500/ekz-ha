# Contributing to EKZ Home Assistant Integration

Danke für dein Interesse an diesem Projekt! Diese Guidelines helfen dir beim Beitragen.

## 🏗️ Architektur

Siehe [ARCHITECTURE.md](custom_components/ekz_ha/ARCHITECTURE.md) für Details zur Code-Struktur.

## 🔧 Development Setup

```bash
# Clone Repository
git clone https://github.com/dmoo500/ekz-ha.git
cd ekz-ha

# Virtual Environment erstellen
python3 -m venv .venv
source .venv/bin/activate  # Linux/Mac
# oder: .venv\Scripts\activate  # Windows

# Dependencies installieren
pip install -e ".[dev]"
```

## 🧪 Tests

```bash
# Alle Tests ausführen
pytest

# Mit Coverage
pytest --cov=custom_components.ekz_ha --cov-report=html

# Spezifischen Test ausführen
pytest tests/api/test_models.py::TestApiValue::test_from_dict_standard_format
```

## 📝 Code Quality

### Linting & Formatting

```bash
# Code formatieren
ruff format custom_components/ tests/

# Linting
ruff check custom_components/ tests/

# Auto-fix
ruff check --fix custom_components/ tests/

# Type Checking
mypy custom_components/ekz_ha/
```

### Pre-Commit Checklist

Vor jedem Commit:

- [ ] Tests laufen durch: `pytest`
- [ ] Linting ist clean: `ruff check custom_components/ tests/`
- [ ] Code ist formatiert: `ruff format custom_components/ tests/`
- [ ] Keine neuen Fehler in Home Assistant Logs

## 🌿 Branching Strategy

### Branch Namen

- `feat/beschreibung` - Neue Features
- `fix/issue-nummer-beschreibung` - Bugfixes
- `refactor/beschreibung` - Code-Refactoring
- `docs/beschreibung` - Dokumentation

### Beispiele

```bash
git checkout -b feat/solar-forecast
git checkout -b fix/issue-23-connection-timeout
git checkout -b refactor/clean-architecture
```

## 📤 Pull Requests

### PR Erstellen

1. **Branch von `main` erstellen**
   ```bash
   git checkout main
   git pull origin main
   git checkout -b feat/mein-feature
   ```

2. **Änderungen committen**
   ```bash
   # Einzelne Dateien hinzufügen (NICHT git add -A)
   git add custom_components/ekz_ha/api/client.py
   git add tests/api/test_client.py
   git commit -m "feat: Add retry logic to API client"
   ```

3. **Push und PR erstellen**
   ```bash
   git push -u origin feat/mein-feature
   ```

### Commit Messages

Format: `<type>: <beschreibung>`

**Types:**
- `feat:` - Neues Feature
- `fix:` - Bugfix
- `refactor:` - Code-Refactoring
- `docs:` - Dokumentation
- `test:` - Tests
- `chore:` - Build, Dependencies, etc.

**Beispiele:**
```
feat: Add solar production forecasting
fix: Resolve session timeout during automatic sync
refactor: Extract aggregation logic to separate module
docs: Update installation instructions
test: Add tests for DST handling
chore: Update dependencies to latest versions
```

### PR Template

Das PR Template wird automatisch geladen. Fülle alle Abschnitte aus:

- Beschreibung der Änderungen
- Art der Änderung (Feature, Bugfix, etc.)
- Bezogene Issues (z.B. "Fixes #18")
- Test-Status
- Checkliste

## 🏷️ Versioning & Releases

### Version Format

**WICHTIG:** Wir verwenden **KEIN** "v" Präfix vor Versionsnummern!

✅ **Richtig:**
```bash
git tag 0.2.0-alpha.1
git tag 0.2.0
git tag 1.0.0
```

❌ **Falsch:**
```bash
git tag v0.2.0-alpha.1  # NICHT VERWENDEN
git tag v0.2.0          # NICHT VERWENDEN
```

### Semantic Versioning

Format: `MAJOR.MINOR.PATCH[-PRERELEASE]`

- **MAJOR** - Breaking Changes
- **MINOR** - Neue Features (backwards compatible)
- **PATCH** - Bugfixes
- **PRERELEASE** - alpha, beta, rc

**Beispiele:**
- `0.1.12` - Patch Release
- `0.2.0` - Minor Release mit neuen Features
- `0.2.0-alpha.1` - Alpha Pre-Release
- `0.2.0-beta.1` - Beta Pre-Release
- `1.0.0` - Major Release

### Release Erstellen

```bash
# 1. Version in manifest.json aktualisieren
# 2. Committen
git add custom_components/ekz_ha/manifest.json
git commit -m "chore: Bump version to 0.2.0"

# 3. Tag erstellen (OHNE "v"!)
git tag 0.2.0

# 4. Push mit Tags
git push origin main
git push origin 0.2.0
```

Der GitHub Workflow erstellt automatisch ein Release mit Changelog.

## 🐛 Bug Reports

Verwende das [Bug Report Template](.github/ISSUE_TEMPLATE/bug_report.yml).

Wichtige Informationen:
- EKZ Integration Version
- Home Assistant Version
- Logs aus Home Assistant
- Schritte zur Reproduktion

## ✨ Feature Requests

Verwende das [Feature Request Template](.github/ISSUE_TEMPLATE/feature_request.yml).

Beschreibe:
- Problem / Motivation
- Vorgeschlagene Lösung
- Alternative Ansätze

## 📚 Dokumentation

- Code-Kommentare auf Deutsch oder Englisch
- Docstrings im Google Style Format
- README und ARCHITECTURE aktuell halten

## 🤝 Code Review

### Was wir prüfen

- Code-Qualität und Lesbarkeit
- Test-Abdeckung
- Performance-Implikationen
- Breaking Changes dokumentiert
- Dokumentation aktualisiert

### Feedback-Prozess

- Reviews konstruktiv und freundlich
- Fragen sind willkommen
- Bei Unsicherheit nachfragen

## 📜 Lizenz

Durch Beiträge stimmst du zu, dass deine Änderungen unter der MIT Lizenz veröffentlicht werden.

## 💬 Kommunikation

- **Issues:** GitHub Issues für Bugs und Feature Requests
- **Diskussionen:** GitHub Discussions für Fragen
- **Pull Requests:** Code Reviews und technische Diskussionen

## 🎯 Best Practices

### Git

```bash
# Einzelne Files hinzufügen (präzise)
git add custom_components/ekz_ha/api/client.py
git add tests/api/test_client.py

# NICHT verwenden
git add -A        # Zu unspezifisch
git add .         # Zu unspezifisch
```

### Python

- Type Hints verwenden
- Docstrings für alle Public Functions/Classes
- Fehlerbehandlung mit spezifischen Exceptions
- Logging statt `print()`
- Konstanten in `const.py`

### Tests

- Ein Test pro Verhalten
- Aussagekräftige Test-Namen
- Arrange-Act-Assert Pattern
- Fixtures für gemeinsame Test-Daten
- Mocking für externe Dependencies

## ❓ Fragen?

Bei Fragen öffne ein Issue oder eine Discussion auf GitHub.

Vielen Dank für deinen Beitrag! 🎉
