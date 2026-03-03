"""Shared envelope utilities for receiver and polling services."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Dict


SCHEMA_VERSION = "1.0"


@dataclass(slots=True)
class Envelope:
    receiver_id: str
    receiver_type: str
    source_ip: str
    source_port: int | None
    protocol: str
    received_at: str
    schema_version: str = SCHEMA_VERSION



def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")



def build_message(
    *,
    receiver_id: str,
    receiver_type: str,
    source_ip: str,
    source_port: int | None,
    protocol: str,
    payload: Any,
    extra_metadata: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    envelope = Envelope(
        receiver_id=receiver_id,
        receiver_type=receiver_type,
        source_ip=source_ip,
        source_port=source_port,
        protocol=protocol,
        received_at=utc_now_iso(),
    )
    result: Dict[str, Any] = {
        "envelope": asdict(envelope),
        "payload": payload,
    }
    if extra_metadata:
        result["metadata"] = extra_metadata
    return result
