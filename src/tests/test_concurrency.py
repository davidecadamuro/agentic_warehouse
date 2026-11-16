"""
Warehouse Swarm Simulation - Concurrency & Race Condition Regression Tests.

Validates that agent singletons handle concurrent events without clobbering
execution context across asynchronous turns.
"""

from __future__ import annotations

import asyncio
import unittest

from agents import PutawayAgent
from broker import InMemoryEventBus
from database import WAREHOUSE_DB
from models import BinStatus, EventType, WarehouseEvent
from tools import register_inbound_pallet


class TestAgentConcurrency(unittest.IsolatedAsyncioTestCase):
    """Test suite ensuring agent singletons process concurrent events without state cross-talk."""

    async def asyncSetUp(self) -> None:
        WAREHOUSE_DB.reset()
        self.bus = InMemoryEventBus()
        await self.bus.start()

        self.putaway_agent = PutawayAgent(event_bus=self.bus, mock_mode=True)
        await self.putaway_agent.initialize()

        self.captured_events: list[tuple[str, WarehouseEvent]] = []

    async def asyncTearDown(self) -> None:
        await self.bus.stop()

    async def _capture_event(self, topic: str, event: WarehouseEvent) -> None:
        self.captured_events.append((topic, event))

    async def test_concurrent_pallet_received_events_do_not_clobber_context(self) -> None:
        """
        When two pallets arrive at the same time, PutawayAgent must process both
        with their respective distinct pallet_ids, weights, and locations without
        overwriting each other's execution context.
        """
        await self.bus.subscribe("warehouse.transport.requested", self._capture_event)

        # 1. Register two distinct pallets
        p1 = register_inbound_pallet(
            sku="SKU-LIGHT-1", quantity=20, weight_kg=100.0, dock_id="DOCK-1"
        )
        p2 = register_inbound_pallet(
            sku="SKU-HEAVY-2", quantity=10, weight_kg=1500.0, dock_id="DOCK-2"
        )

        event1 = WarehouseEvent(
            event_type=EventType.PALLET_RECEIVED,
            payload={
                "pallet_id": p1["pallet_id"],
                "sku": "SKU-LIGHT-1",
                "quantity": 20,
                "weight_kg": 100.0,
                "location": "DOCK-1",
            },
            correlation_id="CORR-P1",
            source_agent="ReceivingAgent",
        )

        event2 = WarehouseEvent(
            event_type=EventType.PALLET_RECEIVED,
            payload={
                "pallet_id": p2["pallet_id"],
                "sku": "SKU-HEAVY-2",
                "quantity": 10,
                "weight_kg": 1500.0,
                "location": "DOCK-2",
            },
            correlation_id="CORR-P2",
            source_agent="ReceivingAgent",
        )

        # 2. Fire both events concurrently into the bus
        await asyncio.gather(
            self.bus.publish("warehouse.inbound.pallet_received", event1),
            self.bus.publish("warehouse.inbound.pallet_received", event2),
        )
        await self.bus.wait_until_idle()

        # 3. Assert both events resulted in separate transport requests
        self.assertEqual(len(self.captured_events), 2)

        emitted_pallets = {e[1].payload["pallet_id"] for e in self.captured_events}
        self.assertIn(p1["pallet_id"], emitted_pallets)
        self.assertIn(p2["pallet_id"], emitted_pallets)

        # 4. Check payload fidelity for both
        for _, event in self.captured_events:
            p_id = event.payload["pallet_id"]
            if p_id == p1["pallet_id"]:
                self.assertEqual(event.payload["source"], "DOCK-1")
                self.assertEqual(event.payload["weight_kg"], 100.0)
                # Light cargo belongs in Zone A
                self.assertTrue(event.payload["destination"].startswith("BIN-A"))
            elif p_id == p2["pallet_id"]:
                self.assertEqual(event.payload["source"], "DOCK-2")
                self.assertEqual(event.payload["weight_kg"], 1500.0)
                # Heavy cargo (>1000kg) belongs in Zone C
                self.assertTrue(event.payload["destination"].startswith("BIN-C"))

        # 5. Check both bins are reserved in DB with no conflicts
        bin1_id = next(e[1].payload["destination"] for e in self.captured_events if e[1].payload["pallet_id"] == p1["pallet_id"])
        bin2_id = next(e[1].payload["destination"] for e in self.captured_events if e[1].payload["pallet_id"] == p2["pallet_id"])
        self.assertNotEqual(bin1_id, bin2_id)
        self.assertEqual(WAREHOUSE_DB.bins[bin1_id].status, BinStatus.RESERVED_PUTAWAY)
        self.assertEqual(WAREHOUSE_DB.bins[bin2_id].status, BinStatus.RESERVED_PUTAWAY)


if __name__ == "__main__":
    unittest.main()
