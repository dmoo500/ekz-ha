"""Verified persistence before irreversible statistics operations."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from homeassistant.core import HomeAssistant
from homeassistant.helpers.storage import Store
from homeassistant.helpers.update_coordinator import UpdateFailed


def _read_saved_document(path: str) -> object:
    with Path(path).open(encoding="utf-8") as handle:
        return json.load(handle)


async def async_save_verified[T: Mapping[str, Any] | Sequence[Any]](
    hass: HomeAssistant, store: Store[T], data: T
) -> None:
    """Read back the file, not Store's in-memory cache, to detect suppressed write errors."""
    await store.async_save(data)
    try:
        document = await hass.async_add_executor_job(_read_saved_document, store.path)
    except (OSError, ValueError) as err:
        raise UpdateFailed(
            f"Could not verify saved statistics state in {store.key}: {err}"
        ) from err
    if (
        not isinstance(document, dict)
        or document.get("version") != store.version
        or document.get("minor_version", 1) != store.minor_version
        or document.get("key") != store.key
        or document.get("data") != data
    ):
        raise UpdateFailed(f"Statistics state was not saved correctly in {store.key}")


async def async_load_verified[T: Mapping[str, Any]](
    hass: HomeAssistant, store: Store[T]
) -> T | None:
    """Reject unreadable files before Store can replace corrupt state with an empty store."""
    try:
        document = await hass.async_add_executor_job(_read_saved_document, store.path)
    except FileNotFoundError:
        return None
    except (OSError, ValueError) as err:
        raise UpdateFailed(f"Could not read saved statistics state in {store.key}: {err}") from err
    if not isinstance(document, dict) or not isinstance(document.get("data"), dict):
        raise UpdateFailed(f"Invalid saved statistics state in {store.key}")
    data: T | None = await store.async_load()
    if data is None:
        raise UpdateFailed(f"Saved statistics state could not be restored from {store.key}")
    return data
