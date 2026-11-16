"""
Warehouse Swarm Simulation - Unit Tests for OrderFulfillmentAgent.
"""

from __future__ import annotations

import unittest

from agents import OrderFulfillmentAgent
from broker import InMemoryEventBus
from database import WAREHOUSE_DB
from models import BinStatus, EventType, WarehouseEvent
from tools import (
    complete_transport_mission,
    dispatch_transport_mission,
    register_inbound_pallet,
    reserve_bin_for_putaway,
)


class TestOrderFulfillmentAgent(unittest.IsolatedAsyncioTestCase):
    """Test suite validating OrderFulfillmentAgent order intake, stock reservation, and pick wave dispatch."""

    async def asyncSetUp(self) -> None:
        WAREHOUSE_DB.reset()
        self.bus = InMemoryEventBus()
        await self.bus.start()

        self.agent = OrderFulfillmentAgent(event_bus=self.bus, mock_mode=True)
        await self.agent.initialize()

        self.captured_events: list[tuple[str, WarehouseEvent]] = []

    async def asyncTearDown(self) -> None:
        await self.bus.stop()

    async def _capture_event(self, topic: str, event: WarehouseEvent) -> None:
        self.captured_events.append((topic, event))

    async def test_order_fulfillment_reserves_stock_and_dispatches_pick(self) -> None:
        """OrderFulfillmentAgent reserves stock from occupied bin and emits PICK_DISPATCHED."""
        await self.bus.subscribe("warehouse.transport.requested", self._capture_event)

        # Pre-seed stock in bin: register, reserve, transport
        reg = register_inbound_pallet(sku="SKU-WIDGET-A", quantity=50, weight_kg=200.0)
        pallet_id = reg["pallet_id"]
        bin_id = "BIN-A-1-1-1"
        reserve_bin_for_putaway(bin_id, pallet_id)
        msn = dispatch_transport_mission(pallet_id, "DOCK-1", bin_id)
        complete_transport_mission(msn["mission_id"])

        # Submit customer order for 20 units
        order_event = WarehouseEvent(
            event_type=EventType.ORDER_SUBMITTED,
            payload={"order_id": "ORD-1001", "sku": "SKU-WIDGET-A", "quantity": 20},
            source_agent="ORDER_API",
        )

        await self.bus.publish("warehouse.outbound.order_submitted", order_event)
        await self.bus.wait_until_idle()

        # Check emitted pick transport request
        self.assertEqual(len(self.captured_events), 1)
        topic, event = self.captured_events[0]
        self.assertEqual(topic, "warehouse.transport.requested")
        self.assertEqual(event.event_type, EventType.PICK_DISPATCHED)
        self.assertEqual(event.payload["order_id"], "ORD-1001")
        self.assertEqual(event.payload["destination"], "PACKING-BAY-1")
        self.assertEqual(event.payload["source"], bin_id)

        # Check bin state transitioned to RESERVED_PICK
        bin_obj = WAREHOUSE_DB.bins[bin_id]
        self.assertEqual(bin_obj.status, BinStatus.RESERVED_PICK)


if __name__ == "__main__":
    unittest.main()
