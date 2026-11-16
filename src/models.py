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
from typing import Any, Dict, Optional


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
    allocated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class LedgerEntry:
    transaction_id: str
    sku: str
    quantity: int
    source: str
    destination: str
    reason: str
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


# ==============================================================================
# Agent Swarm Event Envelope
# ==============================================================================

@dataclass
class WarehouseEvent:
    """Standard message envelope passed through the event bus."""
    event_type: EventType
    payload: Dict[str, Any]
    correlation_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    source_agent: str = "SYSTEM"
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
