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

from models import EventType, WarehouseEvent


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
    across warehouse workflow events.
    """

    @property
    def name(self) -> str:
        return "WarehouseDomainValidator"

    def validate(self, topic: str, event: WarehouseEvent) -> ValidationResult:
        """
        This method requires more sophisticated business intelligence, 
        at the moment the validation checks are a bit naïve. 
        """
        payload = event.payload
        if not isinstance(payload, dict):
            return ValidationResult(is_valid=False, error="Event payload must be a dictionary.")

        event_type = event.event_type

        # 1. Inbound validation: Pallet receipt
        if event_type in (EventType.PALLET_RECEIVED, EventType.PALLET_QUARANTINED):
            pallet_id = payload.get("pallet_id")
            if not pallet_id or not isinstance(pallet_id, str):
                return ValidationResult(is_valid=False, error=f"Missing or invalid 'pallet_id' in {event_type.value}.")

            qty = payload.get("quantity")
            if qty is not None and (not isinstance(qty, int) or qty <= 0):
                return ValidationResult(is_valid=False, error=f"Quantity must be a positive integer, got: {qty}.")

            weight = payload.get("weight_kg")
            if weight is not None and (not isinstance(weight, (int, float)) or weight <= 0.0):
                return ValidationResult(is_valid=False, error=f"Weight must be a positive number, got: {weight}.")

        # 2. Putaway & Transport validation
        elif event_type in (EventType.PUTAWAY_REQUESTED, EventType.TRANSPORT_DISPATCHED, EventType.PICK_DISPATCHED):
            pallet_id = payload.get("pallet_id")
            if not pallet_id:
                return ValidationResult(is_valid=False, error=f"Missing required 'pallet_id' for transport in {event_type.value}.")

            source = payload.get("source")
            destination = payload.get("destination")
            if not source or not destination:
                return ValidationResult(is_valid=False, error="Transport requires both 'source' and 'destination' locations.")

            if source == destination:
                return ValidationResult(is_valid=False, error=f"Invalid transport route: source and destination are identical ('{source}').")

        # 3. Outbound order validation
        elif event_type == EventType.ORDER_SUBMITTED:
            order_id = payload.get("order_id")
            if not order_id:
                return ValidationResult(is_valid=False, error="Missing required 'order_id' in ORDER_SUBMITTED.")

            qty = payload.get("quantity")
            if qty is None or not isinstance(qty, int) or qty <= 0:
                return ValidationResult(is_valid=False, error=f"Order quantity must be a positive integer, got: {qty}.")

        # 4. Corridor anomaly validation
        elif event_type in (EventType.CORRIDOR_BLOCKED, EventType.CORRIDOR_UNBLOCKED):
            zone_id = payload.get("zone_id")
            if not zone_id or zone_id not in {"A", "B", "C"}:
                return ValidationResult(is_valid=False, error=f"Invalid warehouse zone_id '{zone_id}'. Must be A, B, or C.")

        return ValidationResult(is_valid=True)
