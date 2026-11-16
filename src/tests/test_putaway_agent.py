"""
Warehouse Swarm Simulation - Unit Tests for PutawayAgent.
"""

from __future__ import annotations

import unittest

from agents import PutawayAgent
from broker import InMemoryEventBus
from database import WAREHOUSE_DB
from models import BinStatus, EventType, WarehouseEvent
from tools import block_corridor_zone, register_inbound_pallet


class TestPutawayAgent(unittest.IsolatedAsyncioTestCase):
    """Test suite validating PutawayAgent slotting calculations and bin reservations."""

    async def asyncSetUp(self) -> None:
        WAREHOUSE_DB.reset()
        self.bus = InMemoryEventBus()
        await self.bus.start()

        self.agent = PutawayAgent(event_bus=self.bus, mock_mode=True)
        await self.agent.initialize()

        self.captured_events: list[tuple[str, WarehouseEvent]] = []

    async def asyncTearDown(self) -> None:
        await self.bus.stop()

    async def _capture_event(self, topic: str, event: WarehouseEvent) -> None:
        self.captured_events.append((topic, event))

    async def test_pallet_received_reserves_bin_and_requests_transport(self) -> None:
        """PutawayAgent reserves an optimal empty bin and emits PUTAWAY_REQUESTED."""
        await self.bus.subscribe("warehouse.transport.requested", self._capture_event)

        # Register inbound pallet first
        reg = register_inbound_pallet(
            sku="SKU-WIDGET-A", quantity=100, weight_kg=350.0, dock_id="DOCK-1"
        )
        pallet_id = reg["pallet_id"]

        received_event = WarehouseEvent(
            event_type=EventType.PALLET_RECEIVED,
            payload={
                "pallet_id": pallet_id,
                "sku": "SKU-WIDGET-A",
                "quantity": 100,
                "weight_kg": 350.0,
                "location": "DOCK-1",
            },
            source_agent="ReceivingAgent",
        )

        await self.bus.publish("warehouse.inbound.pallet_received", received_event)
        await self.bus.wait_until_idle()

        # Verify transport request was emitted
        self.assertEqual(len(self.captured_events), 1)
        topic, event = self.captured_events[0]
        self.assertEqual(topic, "warehouse.transport.requested")
        self.assertEqual(event.event_type, EventType.PUTAWAY_REQUESTED)

        assigned_bin_id = event.payload["destination"]
        self.assertTrue(assigned_bin_id.startswith("BIN-A"))
        self.assertEqual(event.payload["pallet_id"], pallet_id)
        self.assertEqual(event.payload["source"], "DOCK-1")

        # Verify bin state transitioned to RESERVED_PUTAWAY
        bin_obj = WAREHOUSE_DB.bins[assigned_bin_id]
        self.assertEqual(bin_obj.status, BinStatus.RESERVED_PUTAWAY)
        self.assertEqual(bin_obj.reserved_by, pallet_id)

    async def test_slotting_respects_blocked_zones(self) -> None:
        """PutawayAgent skips bins in blocked zones and picks alternative unblocked bins."""
        await self.bus.subscribe("warehouse.transport.requested", self._capture_event)

        # Block Zone A due to spillage
        block_corridor_zone("A", "Spillage cleanup in progress")

        reg = register_inbound_pallet(
            sku="SKU-WIDGET-A", quantity=50, weight_kg=300.0, dock_id="DOCK-1"
        )
        pallet_id = reg["pallet_id"]

        received_event = WarehouseEvent(
            event_type=EventType.PALLET_RECEIVED,
            payload={
                "pallet_id": pallet_id,
                "sku": "SKU-WIDGET-A",
                "quantity": 50,
                "weight_kg": 300.0,
                "location": "DOCK-1",
            },
            source_agent="ReceivingAgent",
        )

        await self.bus.publish("warehouse.inbound.pallet_received", received_event)
        await self.bus.wait_until_idle()

        self.assertEqual(len(self.captured_events), 1)
        _, event = self.captured_events[0]
        assigned_bin_id = event.payload["destination"]

        # Ensure it detoured to an unblocked zone (Zone B)
        self.assertTrue(assigned_bin_id.startswith("BIN-B"))
        bin_obj = WAREHOUSE_DB.bins[assigned_bin_id]
        self.assertEqual(bin_obj.status, BinStatus.RESERVED_PUTAWAY)


if __name__ == "__main__":
    unittest.main()
