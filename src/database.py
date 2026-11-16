"""
Warehouse Swarm Simulation - Warehouse State Database (Blackboard).

Provides a thread-safe, transactional in-memory state repository modeling
bins, pallets, vehicles, missions, allocations, and the financial/inventory ledger.
"""

from __future__ import annotations

import threading
from collections import deque
from typing import Deque, Dict, List, Set

from models import (
    LedgerEntry,
    Pallet,
    StockAllocation,
    StorageBin,
    TransportMission,
    TransportVehicle,
)


class WarehouseDatabase:
    """Thread-safe transactional in-memory state repository (Blackboard)."""

    def __init__(self, max_ledger_size: int = 5000) -> None:
        self._lock = threading.RLock()
        self._max_ledger_size = max_ledger_size

        # Inbound ASN manifests: asn_id -> dict of expected SKUs and quantities
        self.asn_manifests: Dict[str, Dict[str, int]] = {
            "ASN-101": {"SKU-WIDGET-A": 100, "SKU-GADGET-B": 50},
            "ASN-102": {"SKU-HEAVY-STEEL": 20},
        }

        # Registered Pallets: pallet_id -> Pallet
        self.pallets: Dict[str, Pallet] = {}

        # Bins: bin_id -> StorageBin
        self.bins: Dict[str, StorageBin] = {}
        self._seed_bins()

        # Vehicles: vehicle_id -> TransportVehicle
        self.vehicles: Dict[str, TransportVehicle] = {}
        self._seed_vehicles()

        # Transport Missions: mission_id -> TransportMission
        self.missions: Dict[str, TransportMission] = {}

        # Inventory Allocations: allocation_id -> StockAllocation
        self.allocations: Dict[str, StockAllocation] = {}

        # Financial/Inventory Ledger bounded by circular buffer
        self.ledger: Deque[LedgerEntry] = deque(maxlen=max_ledger_size)

        # Blocked corridors
        self.blocked_zones: Set[str] = set()

    def _seed_bins(self) -> None:
        """Seed a 3-zone warehouse grid."""
        # Zone A (Fast velocity, lighter goods)
        for aisle in range(1, 3):
            for rack in range(1, 4):
                for shelf in range(1, 3):
                    b_id = f"BIN-A-{aisle}-{rack}-{shelf}"
                    self.bins[b_id] = StorageBin(
                        bin_id=b_id,
                        zone="A",
                        aisle=aisle,
                        rack=rack,
                        shelf=shelf,
                        max_weight_kg=500.0,
                    )

        # Zone B (Medium goods)
        for aisle in range(1, 3):
            for rack in range(1, 3):
                b_id = f"BIN-B-{aisle}-{rack}-1"
                self.bins[b_id] = StorageBin(
                    bin_id=b_id,
                    zone="B",
                    aisle=aisle,
                    rack=rack,
                    shelf=1,
                    max_weight_kg=1000.0,
                )

        # Zone C (Heavy / bulk storage)
        for rack in range(1, 3):
            b_id = f"BIN-C-1-{rack}-1"
            self.bins[b_id] = StorageBin(
                bin_id=b_id,
                zone="C",
                aisle=1,
                rack=rack,
                shelf=1,
                max_weight_kg=2500.0,
            )

    def _seed_vehicles(self) -> None:
        """Seed autonomous AGVs and forklifts."""
        self.vehicles["AGV-01"] = TransportVehicle(
            vehicle_id="AGV-01",
            battery_level=95.0,
            payload_capacity_kg=800.0,
            current_location="DOCK-1",
        )
        self.vehicles["AGV-02"] = TransportVehicle(
            vehicle_id="AGV-02",
            battery_level=80.0,
            payload_capacity_kg=800.0,
            current_location="CHARGING-BAY",
        )
        self.vehicles["FORKLIFT-01"] = TransportVehicle(
            vehicle_id="FORKLIFT-01",
            battery_level=100.0,
            payload_capacity_kg=3000.0,
            current_location="ZONE-C",
        )

    def reset(self) -> None:
        """Reset state back to initial seeded baseline for testing/simulations."""
        with self._lock:
            self.pallets.clear()
            self.missions.clear()
            self.allocations.clear()
            self.ledger.clear()
            self.blocked_zones.clear()
            self.bins.clear()
            self._seed_bins()
            self.vehicles.clear()
            self._seed_vehicles()


# Global singleton instance representing the warehouse digital twin / blackboard
WAREHOUSE_DB = WarehouseDatabase()
