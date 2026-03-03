"""Base abstractions for receiver services."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from shared.envelope import build_message


class Publisher(Protocol):
    def publish(self, topic: str, message: dict[str, Any]) -> None:
        ...


@dataclass(slots=True)
class ReceiverContext:
    receiver_id: str
    receiver_type: str
    topic: str


class ReceiverService:
    def __init__(self, context: ReceiverContext, publisher: Publisher) -> None:
        self.context = context
        self.publisher = publisher

    def forward(self, source_ip: str, source_port: int | None, protocol: str, payload: Any) -> None:
        message = build_message(
            receiver_id=self.context.receiver_id,
            receiver_type=self.context.receiver_type,
            source_ip=source_ip,
            source_port=source_port,
            protocol=protocol,
            payload=payload,
        )
        self.publisher.publish(self.context.topic, message)
