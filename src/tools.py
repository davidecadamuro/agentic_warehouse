"""
Warehouse Swarm Simulation - Deterministic Tool Layer.

This module provides the deterministic, transactional tools invoked by agents
during inbound, putaway, transport, picking, and inventory reconciliation workflows.
Agents supply intent; these tools enforce physical invariants and database atomicity.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from database import WAREHOUSE_DB
from models import (
    BinStatus,
    LedgerEntry,
    Pallet,
    PalletCondition,
    StockAllocation,
    StorageBin,
    TransportMission,
    TransportVehicle,
    VehicleStatus,
)


# ==============================================================================
# 1. Inbound & Receiving Tools
# ==============================================================================

def verify_asn(asn_id: str) -> Dict[str, Any]:
    """
    Verifies an Inbound Advance Shipping Notice (ASN).
    Returns expected line items and quantities or an error status.
    """
    with WAREHOUSE_DB._lock:
        if asn_id not in WAREHOUSE_DB.asn_manifests:
            return {
                "valid": False,
                "error": f"ASN '{asn_id}' not found in registry.",
                "manifest": {},
            }
        return {
            "valid": True,
            "asn_id": asn_id,
            "expected_skus": WAREHOUSE_DB.asn_manifests[asn_id],
        }


def register_inbound_pallet(
    sku: str,
    quantity: int,
    weight_kg: float,
    dock_id: str = "DOCK-1",
    condition: PalletCondition = PalletCondition.INTACT,
) -> Dict[str, Any]:
    """
    Registers a newly unloaded pallet at an inbound receiving dock.
    Generates a unique pallet tracking ID and enters it into temporary dock inventory.
    """
    with WAREHOUSE_DB._lock:
        pallet_id = f"PLT-{uuid.uuid4().hex[:8].upper()}"
        pallet = Pallet(
            pallet_id=pallet_id,
            sku=sku,
            quantity=quantity,
            weight_kg=weight_kg,
            condition=condition,
            current_location=dock_id,
        )
        WAREHOUSE_DB.pallets[pallet_id] = pallet

        # Log initial receipt in ledger
        WAREHOUSE_DB.ledger.append(
            LedgerEntry(
                transaction_id=str(uuid.uuid4()),
                sku=sku,
                quantity=quantity,
                source="SUPPLIER_TRUCK",
                destination=dock_id,
                reason=f"Inbound receipt pallet {pallet_id}",
            )
        )

        return {
            "success": True,
            "pallet_id": pallet_id,
            "sku": sku,
            "quantity": quantity,
            "weight_kg": weight_kg,
            "condition": condition.value,
            "location": dock_id,
        }


def quarantine_pallet(pallet_id: str, reason: str) -> Dict[str, Any]:
    """
    Marks a pallet as quarantined if damaged or failing QA.
    Moves it to the inspection quarantine bay.
    """
    with WAREHOUSE_DB._lock:
        pallet = WAREHOUSE_DB.pallets.get(pallet_id)
        if not pallet:
            return {"success": False, "error": f"Pallet '{pallet_id}' not found."}

        pallet.condition = PalletCondition.QUARANTINED
        prev_loc = pallet.current_location
        pallet.current_location = "QUARANTINE-BAY"

        WAREHOUSE_DB.ledger.append(
            LedgerEntry(
                transaction_id=str(uuid.uuid4()),
                sku=pallet.sku,
                quantity=pallet.quantity,
                source=prev_loc,
                destination="QUARANTINE-BAY",
                reason=f"Quarantined: {reason}",
            )
        )

        return {
            "success": True,
            "pallet_id": pallet_id,
            "status": "QUARANTINED",
            "reason": reason,
            "new_location": "QUARANTINE-BAY",
        }


# ==============================================================================
# 2. Putaway & Slotting Tools
# ==============================================================================

def find_candidate_bins(
    weight_kg: float, preferred_zone: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Scans storage bins that are EMPTY, not blocked, and have adequate weight capacity.
    Sorts candidate bins by proximity / zone match.
    """
    candidates = []
    with WAREHOUSE_DB._lock:
        for b in WAREHOUSE_DB.bins.values():
            if b.status != BinStatus.EMPTY:
                continue
            if b.zone in WAREHOUSE_DB.blocked_zones:
                continue
            if b.max_weight_kg < weight_kg:
                continue

            score = 100
            if preferred_zone and b.zone == preferred_zone:
                score += 50
            # Higher shelves preferred for lighter items
            if weight_kg < 200 and b.shelf > 1:
                score += 20

            candidates.append(
                {
                    "bin_id": b.bin_id,
                    "zone": b.zone,
                    "aisle": b.aisle,
                    "rack": b.rack,
                    "shelf": b.shelf,
                    "max_weight_kg": b.max_weight_kg,
                    "score": score,
                }
            )

    candidates.sort(key=lambda x: x["score"], reverse=True)
    return candidates


def reserve_bin_for_putaway(bin_id: str, pallet_id: str) -> Dict[str, Any]:
    """
    Atomically locks a bin from EMPTY to RESERVED_PUTAWAY.
    Guarantees no two agents can claim the same target location.
    """
    with WAREHOUSE_DB._lock:
        b = WAREHOUSE_DB.bins.get(bin_id)
        if not b:
            return {"success": False, "error": f"Bin '{bin_id}' does not exist."}
        if b.status != BinStatus.EMPTY:
            return {
                "success": False,
                "error": f"Bin '{bin_id}' is not EMPTY (current status: {b.status.value}).",
            }

        b.status = BinStatus.RESERVED_PUTAWAY
        b.reserved_by = pallet_id
        return {"success": True, "bin_id": bin_id, "reserved_for": pallet_id}


def release_bin_reservation(bin_id: str) -> Dict[str, Any]:
    """Releases a reservation if putaway is cancelled or rerouted."""
    with WAREHOUSE_DB._lock:
        b = WAREHOUSE_DB.bins.get(bin_id)
        if not b:
            return {"success": False, "error": f"Bin '{bin_id}' not found."}
        if b.status in (BinStatus.RESERVED_PUTAWAY, BinStatus.RESERVED_PICK):
            b.status = BinStatus.EMPTY
            b.reserved_by = None
            return {"success": True, "bin_id": bin_id, "status": "RELEASED"}
        return {"success": False, "error": f"Bin '{bin_id}' is not in a reserved state."}


# ==============================================================================
# 3. Fleet & Transport Tools
# ==============================================================================

def query_available_vehicles(min_capacity_kg: float = 0.0) -> List[Dict[str, Any]]:
    """Returns available transport vehicles ready for mission assignment."""
    results = []
    with WAREHOUSE_DB._lock:
        for v in WAREHOUSE_DB.vehicles.values():
            if (
                v.status == VehicleStatus.IDLE
                and v.battery_level >= 20.0
                and v.payload_capacity_kg >= min_capacity_kg
            ):
                results.append(
                    {
                        "vehicle_id": v.vehicle_id,
                        "battery_level": v.battery_level,
                        "capacity_kg": v.payload_capacity_kg,
                        "location": v.current_location,
                    }
                )
    return results


def dispatch_transport_mission(
    pallet_id: str, source: str, destination: str, preferred_vehicle_id: Optional[str] = None
) -> Dict[str, Any]:
    """
    Assigns an AGV or Forklift to transport a pallet from source to destination.
    Updates vehicle state to BUSY and registers an active mission.
    """
    with WAREHOUSE_DB._lock:
        pallet = WAREHOUSE_DB.pallets.get(pallet_id)
        if not pallet:
            return {"success": False, "error": f"Pallet '{pallet_id}' not found."}

        # Select vehicle
        selected_vehicle: Optional[TransportVehicle] = None
        if preferred_vehicle_id:
            v = WAREHOUSE_DB.vehicles.get(preferred_vehicle_id)
            if v and v.status == VehicleStatus.IDLE and v.payload_capacity_kg >= pallet.weight_kg:
                selected_vehicle = v

        if not selected_vehicle:
            for v in WAREHOUSE_DB.vehicles.values():
                if v.status == VehicleStatus.IDLE and v.payload_capacity_kg >= pallet.weight_kg:
                    selected_vehicle = v
                    break

        if not selected_vehicle:
            return {
                "success": False,
                "error": "No eligible vehicle available with required payload capacity.",
            }

        mission_id = f"MSN-{uuid.uuid4().hex[:8].upper()}"
        mission = TransportMission(
            mission_id=mission_id,
            vehicle_id=selected_vehicle.vehicle_id,
            pallet_id=pallet_id,
            source_location=source,
            destination_location=destination,
            status="IN_TRANSIT",
        )
        WAREHOUSE_DB.missions[mission_id] = mission

        selected_vehicle.status = VehicleStatus.BUSY
        selected_vehicle.current_mission_id = mission_id

        return {
            "success": True,
            "mission_id": mission_id,
            "vehicle_id": selected_vehicle.vehicle_id,
            "pallet_id": pallet_id,
            "source": source,
            "destination": destination,
        }


def complete_transport_mission(mission_id: str) -> Dict[str, Any]:
    """
    Signals that a transport mission has successfully arrived and unloaded.
    Updates the pallet's physical location, bin status, and frees the vehicle.
    """
    with WAREHOUSE_DB._lock:
        mission = WAREHOUSE_DB.missions.get(mission_id)
        if not mission or mission.status != "IN_TRANSIT":
            return {"success": False, "error": f"Invalid or non-active mission '{mission_id}'."}

        vehicle = WAREHOUSE_DB.vehicles.get(mission.vehicle_id)
        pallet = WAREHOUSE_DB.pallets.get(mission.pallet_id)

        if not vehicle or not pallet:
            return {"success": False, "error": "Inconsistent state: vehicle or pallet missing."}

        # Physical location updates
        prev_location = pallet.current_location
        pallet.current_location = mission.destination_location
        vehicle.current_location = mission.destination_location
        vehicle.status = VehicleStatus.IDLE
        vehicle.current_mission_id = None
        vehicle.battery_level = max(0.0, vehicle.battery_level - 3.5)

        mission.status = "COMPLETED"
        mission.completed_at = datetime.now(timezone.utc)

        # If dropped at a bin, update bin occupation
        target_bin = WAREHOUSE_DB.bins.get(mission.destination_location)
        if target_bin:
            target_bin.status = BinStatus.OCCUPIED
            target_bin.current_pallet_id = pallet.pallet_id
            target_bin.reserved_by = None

        # If removed from a bin, free the source bin
        source_bin = WAREHOUSE_DB.bins.get(mission.source_location)
        if source_bin:
            source_bin.status = BinStatus.EMPTY
            source_bin.current_pallet_id = None
            source_bin.reserved_by = None

        # Ledger record
        WAREHOUSE_DB.ledger.append(
            LedgerEntry(
                transaction_id=str(uuid.uuid4()),
                sku=pallet.sku,
                quantity=pallet.quantity,
                source=prev_location,
                destination=mission.destination_location,
                reason=f"Transport mission {mission_id} completed",
            )
        )

        return {
            "success": True,
            "mission_id": mission_id,
            "pallet_id": pallet.pallet_id,
            "new_location": pallet.current_location,
            "vehicle_id": vehicle.vehicle_id,
            "vehicle_status": vehicle.status.value,
        }


# ==============================================================================
# 4. Outbound Order & Picking Tools
# ==============================================================================

def check_and_reserve_stock(sku: str, quantity: int, order_id: str) -> Dict[str, Any]:
    """
    Searches occupied storage bins for matching available stock of a given SKU.
    Atomically allocates pallets and transitions bins to RESERVED_PICK.
    """
    with WAREHOUSE_DB._lock:
        available_pallets: List[Tuple[Pallet, StorageBin]] = []
        for b in WAREHOUSE_DB.bins.values():
            if b.status == BinStatus.OCCUPIED and b.current_pallet_id:
                p = WAREHOUSE_DB.pallets.get(b.current_pallet_id)
                if p and p.sku == sku and p.condition == PalletCondition.INTACT:
                    available_pallets.append((p, b))

        total_avail = sum(p.quantity for p, _ in available_pallets)
        if total_avail < quantity:
            return {
                "success": False,
                "error": f"Insufficient stock for {sku}. Requested: {quantity}, Available: {total_avail}.",
                "available": total_avail,
            }

        remaining_needed = quantity
        allocations = []

        for p, b in available_pallets:
            if remaining_needed <= 0:
                break

            alloc_qty = min(p.quantity, remaining_needed)
            alloc_id = f"ALC-{uuid.uuid4().hex[:8].upper()}"

            alloc = StockAllocation(
                allocation_id=alloc_id,
                order_id=order_id,
                sku=sku,
                quantity=alloc_qty,
                bin_id=b.bin_id,
                pallet_id=p.pallet_id,
            )
            WAREHOUSE_DB.allocations[alloc_id] = alloc

            b.status = BinStatus.RESERVED_PICK
            b.reserved_by = order_id
            remaining_needed -= alloc_qty

            allocations.append(
                {
                    "allocation_id": alloc_id,
                    "bin_id": b.bin_id,
                    "pallet_id": p.pallet_id,
                    "quantity": alloc_qty,
                }
            )

        return {
            "success": True,
            "order_id": order_id,
            "sku": sku,
            "total_reserved": quantity,
            "allocations": allocations,
        }


def pack_and_dispatch_order(order_id: str, dispatch_dock: str = "OUTBOUND-DOCK-1") -> Dict[str, Any]:
    """
    Completes packing for an order, depleting the allocated inventory
    and writing closing entries to the ledger.
    """
    with WAREHOUSE_DB._lock:
        order_allocations = [
            a for a in WAREHOUSE_DB.allocations.values() if a.order_id == order_id
        ]
        if not order_allocations:
            return {"success": False, "error": f"No active allocations found for order '{order_id}'."}

        dispatched_items = []
        for alloc in order_allocations:
            pallet = WAREHOUSE_DB.pallets.get(alloc.pallet_id)
            if pallet:
                pallet.quantity -= alloc.quantity
                if pallet.quantity <= 0:
                    pallet.current_location = "SHIPPED"

                WAREHOUSE_DB.ledger.append(
                    LedgerEntry(
                        transaction_id=str(uuid.uuid4()),
                        sku=alloc.sku,
                        quantity=alloc.quantity,
                        source=alloc.bin_id,
                        destination=dispatch_dock,
                        reason=f"Order fulfillment: {order_id}",
                    )
                )
                dispatched_items.append({"sku": alloc.sku, "quantity": alloc.quantity})

        return {
            "success": True,
            "order_id": order_id,
            "dispatched_to": dispatch_dock,
            "items": dispatched_items,
            "status": "DISPATCHED",
        }


# ==============================================================================
# 5. Inventory & Audit Tools
# ==============================================================================

def get_inventory_summary(sku: Optional[str] = None) -> Dict[str, Any]:
    """
    Returns an aggregated inventory audit across all warehouse locations,
    broken down by status and location.
    """
    with WAREHOUSE_DB._lock:
        summary: Dict[str, Dict[str, Any]] = {}

        for p in WAREHOUSE_DB.pallets.values():
            if sku and p.sku != sku:
                continue
            if p.sku not in summary:
                summary[p.sku] = {
                    "total_quantity": 0,
                    "available_quantity": 0,
                    "quarantined_quantity": 0,
                    "locations": [],
                }

            summary[p.sku]["total_quantity"] += p.quantity
            if p.condition == PalletCondition.INTACT and p.current_location.startswith("BIN"):
                summary[p.sku]["available_quantity"] += p.quantity
            elif p.condition == PalletCondition.QUARANTINED:
                summary[p.sku]["quarantined_quantity"] += p.quantity

            summary[p.sku]["locations"].append(
                {
                    "pallet_id": p.pallet_id,
                    "location": p.current_location,
                    "quantity": p.quantity,
                    "condition": p.condition.value,
                }
            )

        return {"inventory": summary, "timestamp": datetime.now(timezone.utc).isoformat()}


def audit_ledger_history(limit: int = 20) -> List[Dict[str, Any]]:
    """Returns the most recent ledger transactions for tracing consistency."""
    with WAREHOUSE_DB._lock:
        return [
            {
                "tx_id": entry.transaction_id,
                "sku": entry.sku,
                "qty": entry.quantity,
                "from": entry.source,
                "to": entry.destination,
                "reason": entry.reason,
                "timestamp": entry.timestamp.isoformat(),
            }
            for entry in WAREHOUSE_DB.ledger[-limit:]
        ]


# ==============================================================================
# 6. Exception & Anomaly Handling Tools
# ==============================================================================

def block_corridor_zone(zone_id: str, reason: str) -> Dict[str, Any]:
    """
    Blocks a warehouse zone/aisle due to obstacle, spillage, or AGV breakdown.
    Forces putaway and transport agents to detour.
    """
    with WAREHOUSE_DB._lock:
        WAREHOUSE_DB.blocked_zones.add(zone_id)
        return {
            "success": True,
            "blocked_zone": zone_id,
            "reason": reason,
            "active_blocked_zones": list(WAREHOUSE_DB.blocked_zones),
        }


def unblock_corridor_zone(zone_id: str) -> Dict[str, Any]:
    """Clears obstacle and restores corridor access."""
    with WAREHOUSE_DB._lock:
        WAREHOUSE_DB.blocked_zones.discard(zone_id)
        return {"success": True, "unblocked_zone": zone_id}
