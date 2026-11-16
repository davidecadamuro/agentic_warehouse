"""
Warehouse Swarm Simulation - Unit Tests for ExceptionAgent.
"""

from __future__ import annotations

import unittest

from agents import ExceptionAgent
from broker import InMemoryEventBus
from database import WAREHOUSE_DB
from models import EventType, WarehouseEvent


class TestExceptionAgent(unittest.IsolatedAsyncioTestCase):
    """Test suite validating ExceptionAgent corridor blocking, clearance, and safety locks."""

    async def asyncSetUp(self) -> None:
        WAREHOUSE_DB.reset()
        self.bus = InMemoryEventBus()
        await self.bus.start()

        self.agent = ExceptionAgent(event_bus=self.bus, mock_mode=True)
        await self.agent.initialize()

        self.captured_events: list[tuple[str, WarehouseEvent]] = []

    async def asyncTearDown(self) -> None:
        await self.bus.stop()

    async def _capture_event(self, topic: str, event: WarehouseEvent) -> None:
        self.captured_events.append((topic, event))

    async def test_exception_agent_blocks_and_unblocks_corridor(self) -> None:
        """ExceptionAgent cordons off Zone A on obstacle report and restores on clearance."""
        await self.bus.subscribe("warehouse.corridor.blocked", self._capture_event)
        await self.bus.subscribe("warehouse.corridor.unblocked", self._capture_event)

        # Report obstacle
        obs_event = WarehouseEvent(
            event_type=EventType.CORRIDOR_BLOCKED,
            payload={"zone_id": "A", "reason": "AGV battery leakage"},
            source_agent="SENSOR_MONITOR",
        )

        await self.bus.publish("warehouse.exception.obstacle_reported", obs_event)
        await self.bus.wait_until_idle()

        self.assertIn("A", WAREHOUSE_DB.blocked_zones)
        self.assertEqual(len(self.captured_events), 1)
        self.assertEqual(self.captured_events[0][0], "warehouse.corridor.blocked")

        # Clear obstacle
        clear_event = WarehouseEvent(
            event_type=EventType.CORRIDOR_UNBLOCKED,
            payload={"zone_id": "A"},
            source_agent="FACILITY_MAINTENANCE",
        )

        await self.bus.publish("warehouse.exception.obstacle_cleared", clear_event)
        await self.bus.wait_until_idle()

        self.assertNotIn("A", WAREHOUSE_DB.blocked_zones)
        self.assertEqual(len(self.captured_events), 2)
        self.assertEqual(self.captured_events[1][0], "warehouse.corridor.unblocked")


if __name__ == "__main__":
    unittest.main()
