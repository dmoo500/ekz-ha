"""Import progress shared by importers and diagnostic entities."""

from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any


@dataclass
class ImportStatus:
    state: str = "waiting"
    requested_from: date | None = None
    requested_to: date | None = None
    last_attempt: datetime | None = None
    last_result: str | None = None
    error_type: str | None = None

    def begin(self, start: datetime, end: datetime, searching: bool) -> None:
        self.state = "searching" if searching else "importing"
        self.requested_from = start.date()
        self.requested_to = end.date()
        self.last_attempt = datetime.now(tz=UTC)
        self.last_result = None
        self.error_type = None

    def as_attributes(self) -> dict[str, str | None]:
        return {
            "requested_from": self.requested_from.isoformat() if self.requested_from else None,
            "requested_to": self.requested_to.isoformat() if self.requested_to else None,
            "last_attempt": self.last_attempt.isoformat() if self.last_attempt else None,
            "last_result": self.last_result,
            "error_type": self.error_type,
        }

    @classmethod
    def restore(cls, values: dict[str, Any]) -> "ImportStatus":
        return cls(
            state=values["state"],
            requested_from=date.fromisoformat(values["requested_from"])
            if values.get("requested_from")
            else None,
            requested_to=date.fromisoformat(values["requested_to"])
            if values.get("requested_to")
            else None,
            last_attempt=datetime.fromisoformat(values["last_attempt"])
            if values.get("last_attempt")
            else None,
            last_result=values.get("last_result"),
            error_type=values.get("error_type"),
        )


def get_import_status(meta: Any) -> ImportStatus:
    status = getattr(meta, "_import_status", None)
    if not isinstance(status, ImportStatus):
        status = ImportStatus()
        meta._import_status = status
    return status
