"""
Warehouse Swarm Simulation - Unit Tests for TransportAgent.
"""

from __future__ import annotations

import unittest

from agents import TransportAgent
from broker import InMemoryEventBus
from database import WAREHOUSE_DB
from models import BinStatus, EventType, VehicleStatus, WarehouseEvent
from tools import register_inbound_pallet, reserve_bin_for_putaway


class TestTransportAgent(unittest.IsolatedAsyncioTestCase):
    """Test suite validating TransportAgent fleet dispatch and mission execution."""

    async def asyncSetUp(self) -> None:
        WAREHOUSE_DB.reset()
        self.bus = InMemoryEventBus()
        await self.bus.start()

        self.agent = TransportAgent(event_bus=self.bus, mock_mode=True)
        await self.agent.initialize()

        self.captured_events: list[tuple[str, WarehouseEvent]] = []

    async def asyncTearDown(self) -> None:
        await self.bus.stop()

    async def _capture_event(self, topic: str, event: WarehouseEvent) -> None:
        self.captured_events.append((topic, event))

    async def test_transport_requested_dispatches_mission_and_updates_location(self) -> None:
        """TransportAgent assigns an AGV, moves pallet, updates bin status, and emits TRANSPORT_COMPLETED."""
        await self.bus.subscribe("warehouse.transport.completed", self._capture_event)

        # 1. Register pallet at DOCK-1
        reg = register_inbound_pallet(
            sku="SKU-WIDGET-A", quantity=100, weight_kg=350.0, dock_id="DOCK-1"
        )
        pallet_id = reg["pallet_id"]

        # 2. Reserve destination bin
        target_bin_id = "BIN-A-1-1-1"
        reserve_bin_for_putaway(target_bin_id, pallet_id)

        # 3. Request transport
        req_event = WarehouseEvent(
            event_type=EventType.PUTAWAY_REQUESTED,
            payload={
                "pallet_id": pallet_id,
                "sku": "SKU-WIDGET-A",
                "source": "DOCK-1",
                "destination": target_bin_id,
                "weight_kg": 350.0,
            },
            source_agent="PutawayAgent",
        )

        await self.bus.publish("warehouse.transport.requested", req_event)
        await self.bus.wait_until_idle()

        # Check emitted event
        self.assertEqual(len(self.captured_events), 1)
        topic, event = self.captured_events[0]
        self.assertEqual(topic, "warehouse.transport.completed")
        self.assertEqual(event.event_type, EventType.TRANSPORT_COMPLETED)
        self.assertEqual(event.payload["pallet_id"], pallet_id)
        self.assertEqual(event.payload["destination"], target_bin_id)

        # Check physical location and bin occupation in database
        pallet = WAREHOUSE_DB.pallets[pallet_id]
        self.assertEqual(pallet.current_location, target_bin_id)

        bin_obj = WAREHOUSE_DB.bins[target_bin_id]
        self.assertEqual(bin_obj.status, BinStatus.OCCUPIED)
        self.assertEqual(bin_obj.current_pallet_id, pallet_id)

        # Check vehicle state was released back to IDLE
        vehicle_id = event.payload["vehicle_id"]
        vehicle = WAREHOUSE_DB.vehicles[vehicle_id]
        self.assertEqual(vehicle.status, VehicleStatus.IDLE)
        self.assertEqual(vehicle.current_location, target_bin_id)

    async def test_transport_selects_heavy_vehicle_for_heavy_cargo(self) -> None:
        """TransportAgent assigns FORKLIFT-01 when pallet weight exceeds standard AGV capacity (800kg)."""
        await self.bus.subscribe("warehouse.transport.completed", self._capture_event)

        # Register heavy cargo (1500kg)
        reg = register_inbound_pallet(
            sku="SKU-HEAVY-STEEL", quantity=10, weight_kg=1500.0, dock_id="DOCK-1"
        )
        pallet_id = reg["pallet_id"]
        target_bin_id = "BIN-C-1-1-1"
        reserve_bin_for_putaway(target_bin_id, pallet_id)

        req_event = WarehouseEvent(
            event_type=EventType.PUTAWAY_REQUESTED,
            payload={
                "pallet_id": pallet_id,
                "sku": "SKU-HEAVY-STEEL",
                "source": "DOCK-1",
                "destination": target_bin_id,
                "weight_kg": 1500.0,
            },
            source_agent="PutawayAgent",
        )

        await self.bus.publish("warehouse.transport.requested", req_event)
        await self.bus.wait_until_idle()

        self.assertEqual(len(self.captured_events), 1)
        _, event = self.captured_events[0]
        # Must choose FORKLIFT-01 because AGV-01/02 only have 800kg capacity
        self.assertEqual(event.payload["vehicle_id"], "FORKLIFT-01")


if __name__ == "__main__":
    unittest.main()
