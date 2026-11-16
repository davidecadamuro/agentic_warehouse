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

    async def test_partial_pick_triggers_return_transport_and_restores_bin_stock(self) -> None:
        """
        When an order partially picks a pallet (e.g. 20 of 50 units), the remaining 30 units
        trigger a return transport mission back to the source bin, restoring available stock.
        """
        await self.bus.subscribe("warehouse.transport.requested", self._capture_event)
        await self.bus.subscribe("warehouse.outbound.order_dispatched", self._capture_event)

        # 1. Pre-seed pallet with 50 units in BIN-A-1-1-1
        reg = register_inbound_pallet(sku="SKU-PARTIAL", quantity=50, weight_kg=250.0)
        pallet_id = reg["pallet_id"]
        bin_id = "BIN-A-1-1-1"
        reserve_bin_for_putaway(bin_id, pallet_id)
        msn = dispatch_transport_mission(pallet_id, "DOCK-1", bin_id)
        complete_transport_mission(msn["mission_id"])

        # 2. Place order for 20 units
        order_event = WarehouseEvent(
            event_type=EventType.ORDER_SUBMITTED,
            payload={"order_id": "ORD-PARTIAL-1", "sku": "SKU-PARTIAL", "quantity": 20},
            source_agent="ORDER_API",
        )
        await self.bus.publish("warehouse.outbound.order_submitted", order_event)
        await self.bus.wait_until_idle()

        # Pick transport was emitted
        pick_reqs = [e for t, e in self.captured_events if t == "warehouse.transport.requested" and e.payload["destination"] == "PACKING-BAY-1"]
        self.assertEqual(len(pick_reqs), 1)

        # 3. Simulate AGV delivering pallet to PACKING-BAY-1
        msn_pick = dispatch_transport_mission(pallet_id, bin_id, "PACKING-BAY-1")
        complete_transport_mission(msn_pick["mission_id"])

        # Source bin should be reserved for putaway return (not freed to EMPTY)
        self.assertEqual(WAREHOUSE_DB.bins[bin_id].status, BinStatus.RESERVED_PUTAWAY)

        # Notify OrderFulfillmentAgent that pallet arrived at PACKING-BAY-1
        arrival_event = WarehouseEvent(
            event_type=EventType.TRANSPORT_COMPLETED,
            payload={
                "mission_id": msn_pick["mission_id"],
                "pallet_id": pallet_id,
                "source": bin_id,
                "destination": "PACKING-BAY-1",
            },
            source_agent="TransportAgent",
        )
        await self.bus.publish("warehouse.transport.completed", arrival_event)
        await self.bus.wait_until_idle()

        # 4. Check that order was dispatched and return transport was requested
        dispatched_events = [e for t, e in self.captured_events if t == "warehouse.outbound.order_dispatched"]
        self.assertEqual(len(dispatched_events), 1)
        self.assertEqual(dispatched_events[0].payload["order_id"], "ORD-PARTIAL-1")

        return_reqs = [e for t, e in self.captured_events if t == "warehouse.transport.requested" and e.payload["destination"] == bin_id]
        self.assertEqual(len(return_reqs), 1)
        return_req = return_reqs[0]
        self.assertEqual(return_req.payload["pallet_id"], pallet_id)
        self.assertEqual(return_req.payload["source"], "PACKING-BAY-1")
        self.assertEqual(return_req.payload["destination"], bin_id)

        # 5. Complete return transport back to rack
        msn_return = dispatch_transport_mission(pallet_id, "PACKING-BAY-1", bin_id)
        complete_transport_mission(msn_return["mission_id"])

        # 6. Verify inventory and bin state are fully restored
        bin_obj = WAREHOUSE_DB.bins[bin_id]
        self.assertEqual(bin_obj.status, BinStatus.OCCUPIED)
        self.assertEqual(bin_obj.current_pallet_id, pallet_id)

        pallet = WAREHOUSE_DB.pallets[pallet_id]
        self.assertEqual(pallet.quantity, 30)
        self.assertEqual(pallet.current_location, bin_id)


if __name__ == "__main__":
    unittest.main()
