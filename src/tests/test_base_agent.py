"""
Warehouse Swarm Simulation - Unit Tests for BaseAgent publish and subscribe helpers.
"""

from __future__ import annotations

import unittest
from typing import Optional

from agents.base_agent import BaseAgent
from broker import InMemoryEventBus
from models import EventType, WarehouseEvent


class DummyTestAgent(BaseAgent):
    """Minimal concrete subclass of BaseAgent to test base mechanics."""

    def __init__(self, event_bus: InMemoryEventBus) -> None:
        super().__init__(
            name="DummyTestAgent",
            system_prompt="You are a dummy test agent.",
            event_bus=event_bus,
            mock_mode=True,
        )

    async def initialize(self) -> None:
        pass

    async def mock_reasoning(
        self, prompt: str, correlation_id: Optional[str] = None
    ) -> str:
        return f"Dummy response to {prompt}"


class TestBaseAgent(unittest.IsolatedAsyncioTestCase):
    """Test suite validating BaseAgent publish, subscribe, and event attribution."""

    async def asyncSetUp(self) -> None:
        self.bus = InMemoryEventBus()
        await self.bus.start()
        self.agent = DummyTestAgent(event_bus=self.bus)
        await self.agent.initialize()
        self.received_events: list[tuple[str, WarehouseEvent]] = []

    async def asyncTearDown(self) -> None:
        await self.bus.stop()

    async def _sample_handler(self, topic: str, event: WarehouseEvent) -> None:
        self.received_events.append((topic, event))

    async def test_agent_subscribe_registers_handler_on_broker(self) -> None:
        """Calling self.subscribe binds the handler to the event bus."""
        await self.agent.subscribe("warehouse.test.topic", self._sample_handler)

        event = WarehouseEvent(
            event_type=EventType.PALLET_RECEIVED,
            payload={"pallet_id": "PLT-DUMMY", "test": "data"},
        )
        await self.bus.publish("warehouse.test.topic", event)
        await self.bus.wait_until_idle()

        self.assertEqual(len(self.received_events), 1)
        topic, received = self.received_events[0]
        self.assertEqual(topic, "warehouse.test.topic")
        self.assertEqual(received.payload["test"], "data")

    async def test_agent_publish_attributes_source_agent_automatically(self) -> None:
        """Calling self.publish sets source_agent to the agent's name if omitted or default."""
        await self.bus.subscribe("warehouse.test.attribution", self._sample_handler)

        event = WarehouseEvent(
            event_type=EventType.PALLET_RECEIVED,
            payload={"pallet_id": "PLT-DUMMY", "msg": "attributed"},
            source_agent="SYSTEM",  # Default system attribution should be replaced
        )

        await self.agent.publish("warehouse.test.attribution", event)
        await self.bus.wait_until_idle()

        self.assertEqual(len(self.received_events), 1)
        _, received = self.received_events[0]
        self.assertEqual(received.source_agent, "DummyTestAgent")

    async def test_agent_publish_preserves_explicit_custom_source_agent(self) -> None:
        """Calling self.publish preserves custom source_agent if explicitly provided."""
        await self.bus.subscribe("warehouse.test.custom_source", self._sample_handler)

        event = WarehouseEvent(
            event_type=EventType.PALLET_RECEIVED,
            payload={"pallet_id": "PLT-DUMMY", "msg": "custom"},
            source_agent="CUSTOM_PRODUCER",
        )

        await self.agent.publish("warehouse.test.custom_source", event)
        await self.bus.wait_until_idle()

        self.assertEqual(len(self.received_events), 1)
        _, received = self.received_events[0]
        self.assertEqual(received.source_agent, "CUSTOM_PRODUCER")


if __name__ == "__main__":
    unittest.main()
