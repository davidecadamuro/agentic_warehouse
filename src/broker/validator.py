"""
Warehouse Swarm Simulation - Inter-Stage Event Validation & Guardrails.

Provides validation gates to intercept events published by agents before they are
dispatched across the broker. Guarantees that agent hallucinations, missing fields,
or impossible physical values (negative weights, zero quantities, invalid locations)
are blocked and audited before reaching downstream workflow stages.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional

from pydantic import BaseModel, ValidationError

from models import EVENT_PAYLOAD_MODELS, EventType, WarehouseEvent


@dataclass
class ValidationResult:
    """Outcome of an inter-stage validation check."""
    is_valid: bool
    error: Optional[str] = None


class EventValidator(ABC):
    """Abstract interface for inter-stage event validation gates."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Human-readable identifier for the validator gate."""
        pass

    @abstractmethod
    def validate(self, topic: str, event: WarehouseEvent) -> ValidationResult:
        """
        Validates an event before it is enqueued for downstream consumption.

        Args:
            topic: Published routing topic.
            event: The proposed WarehouseEvent envelope.

        Returns:
            ValidationResult(is_valid=True) if valid, or ValidationResult(is_valid=False, error=...)
        """
        pass


class WarehouseDomainValidator(EventValidator):
    """
    Core domain guardrail verifying physical plausibility and contract invariants
    across warehouse workflow events using typed Pydantic schemas.
    """

    @property
    def name(self) -> str:
        return "WarehouseDomainValidator"

    def validate(self, topic: str, event: WarehouseEvent) -> ValidationResult:
        """
        Validates event payload against the registered Pydantic contract for its EventType.
        Catches invalid formats, negative quantities/weights, identical routes, and illegal zones.
        """
        raw_payload = event.payload
        if isinstance(raw_payload, BaseModel):
            payload_data = raw_payload.model_dump()
        elif isinstance(raw_payload, dict):
            payload_data = raw_payload
        else:
            return ValidationResult(is_valid=False, error="Event payload must be a dictionary or BaseModel instance.")

        target_model = EVENT_PAYLOAD_MODELS.get(event.event_type)
        if not target_model:
            # If no strict model registered for this event type, accept as valid
            return ValidationResult(is_valid=True)

        try:
            target_model.model_validate(payload_data)
            return ValidationResult(is_valid=True)
        except ValidationError as err:
            # Extract first human-readable error message
            first_err = err.errors()[0]
            field_name = ".".join(str(loc) for loc in first_err.get("loc", []))
            msg = first_err.get("msg", "Validation error")
            
            # Clean up Pydantic 'Value error, ' prefix if present
            if msg.startswith("Value error, "):
                clean_msg = msg[len("Value error, "):]
            elif field_name:
                clean_msg = f"{field_name}: {msg}"
            else:
                clean_msg = msg

            return ValidationResult(is_valid=False, error=clean_msg)
