"""
Warehouse Swarm Simulation - Unit Tests for ReceivingAgent and Broker.
"""

from __future__ import annotations

import asyncio
import unittest

from agents import ReceivingAgent
from broker import InMemoryEventBus
from database import WAREHOUSE_DB
from models import EventType, PalletCondition, WarehouseEvent


class TestReceivingAgent(unittest.IsolatedAsyncioTestCase):
    """Test suite for ReceivingAgent event intake and tool invocation."""

    async def asyncSetUp(self) -> None:
        WAREHOUSE_DB.reset()
        self.bus = InMemoryEventBus()
        await self.bus.start()

        self.agent = ReceivingAgent(event_bus=self.bus, mock_mode=True)
        await self.agent.initialize()

        self.captured_events: list[tuple[str, WarehouseEvent]] = []

    async def asyncTearDown(self) -> None:
        await self.bus.stop()

    async def _capture_event(self, topic: str, event: WarehouseEvent) -> None:
        self.captured_events.append((topic, event))

    async def test_shipment_arrived_intact_registers_and_emits_event(self) -> None:
        """When an intact shipment arrives, ReceivingAgent registers it and emits PALLET_RECEIVED."""
        await self.bus.subscribe("warehouse.inbound.pallet_received", self._capture_event)

        arrival_event = WarehouseEvent(
            event_type=EventType.SHIPMENT_ARRIVED,
            payload={
                "asn_id": "ASN-101",
                "pallets": [
                    {"sku": "SKU-WIDGET-A", "qty": 100, "weight_kg": 350.0}
                ],
            },
            source_agent="TEST_DOCK",
        )

        await self.bus.publish("warehouse.inbound.shipment_arrived", arrival_event)
        await self.bus.wait_until_idle()

        # Check emitted event
        self.assertEqual(len(self.captured_events), 1)
        topic, event = self.captured_events[0]
        self.assertEqual(topic, "warehouse.inbound.pallet_received")
        self.assertEqual(event.event_type, EventType.PALLET_RECEIVED)

        pallet_id = event.payload["pallet_id"]
        self.assertEqual(event.payload["sku"], "SKU-WIDGET-A")
        self.assertEqual(event.payload["quantity"], 100)
        self.assertEqual(event.payload["condition"], PalletCondition.INTACT.value)

        # Check database consistency
        self.assertIn(pallet_id, WAREHOUSE_DB.pallets)
        registered_pallet = WAREHOUSE_DB.pallets[pallet_id]
        self.assertEqual(registered_pallet.current_location, "DOCK-1")

    async def test_damaged_shipment_quarantines_pallet(self) -> None:
        """When a damaged shipment arrives, ReceivingAgent quarantines it and emits PALLET_QUARANTINED."""
        await self.bus.subscribe("warehouse.inbound.pallet_quarantined", self._capture_event)

        arrival_event = WarehouseEvent(
            event_type=EventType.SHIPMENT_ARRIVED,
            payload={
                "asn_id": "ASN-101",
                "pallets": [
                    {"sku": "SKU-FRAGILE", "qty": 10, "weight_kg": 150.0, "condition": "DAMAGED"}
                ],
            },
            source_agent="TEST_DOCK",
        )

        await self.bus.publish("warehouse.inbound.shipment_arrived", arrival_event)
        await self.bus.wait_until_idle()

        self.assertEqual(len(self.captured_events), 1)
        topic, event = self.captured_events[0]
        self.assertEqual(topic, "warehouse.inbound.pallet_quarantined")
        self.assertEqual(event.event_type, EventType.PALLET_QUARANTINED)
        self.assertEqual(event.payload["status"], "QUARANTINED")
        self.assertEqual(event.payload["new_location"], "QUARANTINE-BAY")


if __name__ == "__main__":
    unittest.main()
