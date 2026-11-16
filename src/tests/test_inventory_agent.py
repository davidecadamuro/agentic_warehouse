"""
Warehouse Swarm Simulation - Unit Tests for InventoryAgent.
"""

from __future__ import annotations

import unittest

from agents import InventoryAgent
from broker import InMemoryEventBus
from database import WAREHOUSE_DB
from models import EventType, WarehouseEvent
from tools import (
    complete_transport_mission,
    dispatch_transport_mission,
    register_inbound_pallet,
    reserve_bin_for_putaway,
)


class TestInventoryAgent(unittest.IsolatedAsyncioTestCase):
    """Test suite validating InventoryAgent stock auditing and low-stock alerts."""

    async def asyncSetUp(self) -> None:
        WAREHOUSE_DB.reset()
        self.bus = InMemoryEventBus()
        await self.bus.start()

        self.agent = InventoryAgent(event_bus=self.bus, mock_mode=True, replenishment_threshold=15)
        await self.agent.initialize()

        self.captured_events: list[tuple[str, WarehouseEvent]] = []

    async def asyncTearDown(self) -> None:
        await self.bus.stop()

    async def _capture_event(self, topic: str, event: WarehouseEvent) -> None:
        self.captured_events.append((topic, event))

    async def test_inventory_agent_emits_replenishment_alert_when_stock_low(self) -> None:
        """InventoryAgent detects available stock below threshold and publishes replenishment alert."""
        await self.bus.subscribe("warehouse.inventory.replenishment_alert", self._capture_event)

        # Pre-seed low quantity pallet (10 units < threshold 15) in bin
        reg = register_inbound_pallet(sku="SKU-WIDGET-A", quantity=10, weight_kg=100.0)
        pallet_id = reg["pallet_id"]
        bin_id = "BIN-A-1-1-1"
        reserve_bin_for_putaway(bin_id, pallet_id)
        msn = dispatch_transport_mission(pallet_id, "DOCK-1", bin_id)
        complete_transport_mission(msn["mission_id"])

        # Request audit
        audit_event = WarehouseEvent(
            event_type=EventType.INVENTORY_MISMATCH,
            payload={"sku": "SKU-WIDGET-A"},
            source_agent="AUDIT_SYSTEM",
        )

        await self.bus.publish("warehouse.inventory.audit_requested", audit_event)
        await self.bus.wait_until_idle()

        self.assertEqual(len(self.captured_events), 1)
        topic, event = self.captured_events[0]
        self.assertEqual(topic, "warehouse.inventory.replenishment_alert")
        self.assertEqual(event.payload["sku"], "SKU-WIDGET-A")
        self.assertEqual(event.payload["available_quantity"], 10)


if __name__ == "__main__":
    unittest.main()
