"""
Warehouse Swarm Simulation - Domain Models and Event Contracts.

Contains core entities representing physical assets (bins, pallets, vehicles),
runtime operations (missions, allocations, ledger entries), and event message envelopes
exchanged between swarm agents over the event bus.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Generic, List, Optional, Type, TypeVar, Union

from pydantic import BaseModel, ConfigDict, Field, model_validator


# ==============================================================================
# Domain Enums
# ==============================================================================

class BinStatus(str, Enum):
    EMPTY = "EMPTY"
    RESERVED_PUTAWAY = "RESERVED_PUTAWAY"
    OCCUPIED = "OCCUPIED"
    RESERVED_PICK = "RESERVED_PICK"
    BLOCKED = "BLOCKED"


class VehicleStatus(str, Enum):
    IDLE = "IDLE"
    BUSY = "BUSY"
    MAINTENANCE = "MAINTENANCE"
    CHARGING = "CHARGING"


class PalletCondition(str, Enum):
    INTACT = "INTACT"
    DAMAGED = "DAMAGED"
    QUARANTINED = "QUARANTINED"


class EventType(str, Enum):
    # Inbound
    SHIPMENT_ARRIVED = "SHIPMENT_ARRIVED"
    PALLET_RECEIVED = "PALLET_RECEIVED"
    PALLET_QUARANTINED = "PALLET_QUARANTINED"

    # Putaway
    BIN_RESERVED = "BIN_RESERVED"
    PUTAWAY_REQUESTED = "PUTAWAY_REQUESTED"

    # Transport / Fleet
    TRANSPORT_DISPATCHED = "TRANSPORT_DISPATCHED"
    TRANSPORT_COMPLETED = "TRANSPORT_COMPLETED"
    CORRIDOR_BLOCKED = "CORRIDOR_BLOCKED"
    CORRIDOR_UNBLOCKED = "CORRIDOR_UNBLOCKED"

    # Outbound / Picking
    ORDER_SUBMITTED = "ORDER_SUBMITTED"
    STOCK_RESERVED = "STOCK_RESERVED"
    PICK_DISPATCHED = "PICK_DISPATCHED"
    ORDER_DISPATCHED = "ORDER_DISPATCHED"

    # Exceptions
    INVENTORY_MISMATCH = "INVENTORY_MISMATCH"
    TRANSPORT_STALLED = "TRANSPORT_STALLED"


# ==============================================================================
# Warehouse Physical & Operational Entities
# ==============================================================================

@dataclass
class StorageBin:
    bin_id: str
    zone: str
    aisle: int
    rack: int
    shelf: int
    max_weight_kg: float
    status: BinStatus = BinStatus.EMPTY
    current_pallet_id: Optional[str] = None
    reserved_by: Optional[str] = None


@dataclass
class Pallet:
    pallet_id: str
    sku: str
    quantity: int
    weight_kg: float
    condition: PalletCondition
    current_location: str
    registered_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class TransportVehicle:
    vehicle_id: str
    battery_level: float  # 0.0 to 100.0
    payload_capacity_kg: float
    current_location: str
    status: VehicleStatus = VehicleStatus.IDLE
    current_mission_id: Optional[str] = None


@dataclass
class TransportMission:
    mission_id: str
    vehicle_id: str
    pallet_id: str
    source_location: str
    destination_location: str
    status: str  # "ASSIGNED", "IN_TRANSIT", "COMPLETED", "FAILED"
    correlation_id: Optional[str] = None
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    completed_at: Optional[datetime] = None


@dataclass
class StockAllocation:
    allocation_id: str
    order_id: str
    sku: str
    quantity: int
    bin_id: str
    pallet_id: str
    correlation_id: Optional[str] = None
    allocated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class LedgerEntry:
    transaction_id: str
    sku: str
    quantity: int
    source: str
    destination: str
    reason: str
    correlation_id: Optional[str] = None
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


# ==============================================================================
# Strongly Typed Event Payload Models (Pydantic V2)
# ==============================================================================

class BaseEventPayload(BaseModel):
    """Base model for all event payloads with extra field tolerance."""
    model_config = ConfigDict(extra="allow", populate_by_name=True)


class ShipmentPalletItem(BaseModel):
    sku: str
    qty: int = Field(gt=0, description="Quantity must be strictly positive")
    weight_kg: float = Field(gt=0.0, description="Weight must be strictly positive")
    condition: Optional[str] = "INTACT"


class ShipmentArrivedPayload(BaseEventPayload):
    asn_id: str
    pallets: List[Dict[str, Any]] = Field(default_factory=list)


class PalletReceivedPayload(BaseEventPayload):
    pallet_id: str = Field(min_length=1)
    sku: Optional[str] = "SKU-UNKNOWN"
    quantity: Optional[int] = Field(default=1, gt=0, description="Pallet quantity must be positive")
    weight_kg: Optional[float] = Field(default=100.0, gt=0.0, description="Pallet weight must be positive")
    location: str = "DOCK-1"
    condition: str = "INTACT"
    success: Optional[bool] = True


class PalletQuarantinedPayload(BaseEventPayload):
    pallet_id: str = Field(min_length=1)
    status: str = "QUARANTINED"
    reason: str
    new_location: str = "QUARANTINE-BAY"
    success: Optional[bool] = True


class TransportRequestedPayload(BaseEventPayload):
    pallet_id: str = Field(min_length=1)
    source: str = Field(min_length=1)
    destination: str = Field(min_length=1)
    sku: Optional[str] = "SKU-UNKNOWN"
    weight_kg: Optional[float] = Field(default=300.0, gt=0.0)
    order_id: Optional[str] = None

    @model_validator(mode="after")
    def verify_distinct_route(self) -> TransportRequestedPayload:
        if self.source == self.destination:
            raise ValueError(f"Invalid transport route: source and destination are identical ('{self.source}').")
        return self


class TransportCompletedPayload(BaseEventPayload):
    mission_id: Optional[str] = None
    pallet_id: str = Field(min_length=1)
    vehicle_id: Optional[str] = "AGV-01"
    source: Optional[str] = "UNKNOWN"
    destination: str = Field(min_length=1)


class OrderSubmittedPayload(BaseEventPayload):
    order_id: str = Field(min_length=1)
    sku: str
    quantity: int = Field(gt=0, description="Order quantity must be positive")


class OrderDispatchedPayload(BaseEventPayload):
    order_id: str
    dispatched_to: str
    items: List[Dict[str, Any]]
    residual_pallets: Optional[List[Dict[str, Any]]] = Field(default_factory=list)
    status: str = "DISPATCHED"
    success: Optional[bool] = True


class CorridorBlockedPayload(BaseEventPayload):
    zone_id: str
    reason: str

    @model_validator(mode="after")
    def verify_zone(self) -> CorridorBlockedPayload:
        if self.zone_id not in {"A", "B", "C"}:
            raise ValueError(f"Invalid warehouse zone_id '{self.zone_id}'. Must be A, B, or C.")
        return self


class CorridorUnblockedPayload(BaseEventPayload):
    zone_id: str

    @model_validator(mode="after")
    def verify_zone(self) -> CorridorUnblockedPayload:
        if self.zone_id not in {"A", "B", "C"}:
            raise ValueError(f"Invalid warehouse zone_id '{self.zone_id}'. Must be A, B, or C.")
        return self


class ReplenishmentAlertPayload(BaseEventPayload):
    sku: Optional[str] = None
    available_quantity: Optional[int] = None
    threshold: Optional[int] = None


# Mapping EventType to expected typed payload model for automated validation
EVENT_PAYLOAD_MODELS: Dict[EventType, Type[BaseModel]] = {
    EventType.SHIPMENT_ARRIVED: ShipmentArrivedPayload,
    EventType.PALLET_RECEIVED: PalletReceivedPayload,
    EventType.PALLET_QUARANTINED: PalletQuarantinedPayload,
    EventType.PUTAWAY_REQUESTED: TransportRequestedPayload,
    EventType.PICK_DISPATCHED: TransportRequestedPayload,
    EventType.TRANSPORT_DISPATCHED: TransportRequestedPayload,
    EventType.TRANSPORT_COMPLETED: TransportCompletedPayload,
    EventType.ORDER_SUBMITTED: OrderSubmittedPayload,
    EventType.ORDER_DISPATCHED: OrderDispatchedPayload,
    EventType.CORRIDOR_BLOCKED: CorridorBlockedPayload,
    EventType.CORRIDOR_UNBLOCKED: CorridorUnblockedPayload,
    EventType.INVENTORY_MISMATCH: ReplenishmentAlertPayload,
}

PayloadT = TypeVar("PayloadT", bound=Union[BaseModel, Dict[str, Any]])


# ==============================================================================
# Agent Swarm Event Envelope
# ==============================================================================

@dataclass
class WarehouseEvent(Generic[PayloadT]):
    """
    Standard message envelope passed through the event bus.
    Payload can be either a typed Pydantic model or a dictionary.
    Exposes dict-like and model-like helpers for seamless compatibility.
    """
    event_type: EventType
    payload: PayloadT
    correlation_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    source_agent: str = "SYSTEM"
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def payload_dict(self) -> Dict[str, Any]:
        """Returns payload as a standard Python dictionary."""
        if isinstance(self.payload, BaseModel):
            return self.payload.model_dump()
        return dict(self.payload)
