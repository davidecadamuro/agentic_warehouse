"""
Warehouse Swarm Simulation - Unit Tests for Broker.
"""

from __future__ import annotations

import unittest

from broker import InMemoryEventBus
from models import EventType, WarehouseEvent


class TestInMemoryBroker(unittest.IsolatedAsyncioTestCase):
    """Test suite for InMemoryEventBus wildcard routing and error handling."""

    async def asyncSetUp(self) -> None:
        self.bus = InMemoryEventBus()
        await self.bus.start()
        self.received: list[tuple[str, WarehouseEvent]] = []

    async def asyncTearDown(self) -> None:
        await self.bus.stop()

    async def _handler(self, topic: str, event: WarehouseEvent) -> None:
        self.received.append((topic, event))

    async def test_single_word_wildcard_routing(self) -> None:
        """Star (*) matches exactly one word token."""
        await self.bus.subscribe("warehouse.inbound.*", self._handler)

        event = WarehouseEvent(
            event_type=EventType.PALLET_RECEIVED,
            payload={"pallet_id": "PLT-1", "test": "data"},
        )

        # Matching topic
        await self.bus.publish("warehouse.inbound.pallet_received", event)
        # Non-matching topic (2 tokens deeper)
        await self.bus.publish("warehouse.inbound.dock1.pallet_received", event)
        await self.bus.wait_until_idle()

        self.assertEqual(len(self.received), 1)
        self.assertEqual(self.received[0][0], "warehouse.inbound.pallet_received")

    async def test_multi_word_wildcard_routing(self) -> None:
        """Hash (#) matches zero or more word tokens."""
        await self.bus.subscribe("warehouse.transport.#", self._handler)

        event = WarehouseEvent(
            event_type=EventType.TRANSPORT_DISPATCHED,
            payload={"mission_id": "MSN-123", "pallet_id": "PLT-1", "source": "DOCK-1", "destination": "BIN-A-1-1-1"},
        )

        await self.bus.publish("warehouse.transport.requested", event)
        await self.bus.publish("warehouse.transport.agv.mission.completed", event)
        await self.bus.wait_until_idle()

        self.assertEqual(len(self.received), 2)

    async def test_dead_letter_queue_captures_exceptions(self) -> None:
        """Failing handlers are caught and isolated in the DLQ."""
        async def failing_handler(topic: str, event: WarehouseEvent) -> None:
            raise ValueError("Intentional simulated consumer crash")

        await self.bus.subscribe("warehouse.faulty.*", failing_handler)

        event = WarehouseEvent(
            event_type=EventType.INVENTORY_MISMATCH,
            payload={"error": "corrupted"},
        )

        await self.bus.publish("warehouse.faulty.event", event)
        await self.bus.wait_until_idle()

        self.assertEqual(len(self.bus.dlq), 1)
        dlq_entry = self.bus.dlq[0]
        self.assertEqual(dlq_entry.topic, "warehouse.faulty.event")
        self.assertIn("Intentional simulated consumer crash", dlq_entry.error)

    async def test_validator_gate_blocks_hallucinated_and_invalid_events(self) -> None:
        """Events violating physical invariants are blocked before downstream dispatch."""
        downstream_events = []

        async def downstream_listener(topic: str, event: WarehouseEvent) -> None:
            downstream_events.append((topic, event))

        await self.bus.subscribe("warehouse.inbound.pallet_received", downstream_listener)

        # 1. Invalid event: negative quantity (impossible physics)
        invalid_event = WarehouseEvent(
            event_type=EventType.PALLET_RECEIVED,
            payload={"pallet_id": "PLT-BAD", "quantity": -50, "weight_kg": 200.0},
        )

        await self.bus.publish("warehouse.inbound.pallet_received", invalid_event)
        await self.bus.wait_until_idle()

        # Must not reach downstream subscriber
        self.assertEqual(len(downstream_events), 0)
        # Must be recorded in rejected_events
        self.assertEqual(len(self.bus.rejected_events), 1)
        topic, ev, reason = self.bus.rejected_events[0]
        self.assertEqual(topic, "warehouse.inbound.pallet_received")
        self.assertIn("Quantity must be a positive integer", reason)

    async def test_validator_gate_blocks_identical_source_and_destination(self) -> None:
        """Transport event with identical source and destination is caught and rejected."""
        downstream_events = []

        async def downstream_listener(topic: str, event: WarehouseEvent) -> None:
            downstream_events.append((topic, event))

        await self.bus.subscribe("warehouse.transport.requested", downstream_listener)

        invalid_route_event = WarehouseEvent(
            event_type=EventType.PUTAWAY_REQUESTED,
            payload={"pallet_id": "PLT-100", "source": "DOCK-1", "destination": "DOCK-1"},
        )

        await self.bus.publish("warehouse.transport.requested", invalid_route_event)
        await self.bus.wait_until_idle()

        self.assertEqual(len(downstream_events), 0)
        self.assertEqual(len(self.bus.rejected_events), 1)
        self.assertIn("source and destination are identical", self.bus.rejected_events[0][2])


if __name__ == "__main__":
    unittest.main()
