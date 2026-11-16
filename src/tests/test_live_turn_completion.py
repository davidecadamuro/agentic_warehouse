"""
Warehouse Swarm Simulation - Unit Tests for Live LLM Mode Turn Completion Hook.

Tests that when running in live mode (with an OpenAI client), tool invocations
trigger on_turn_completed and emit downstream events to the broker.
"""

from __future__ import annotations

import json
import unittest
from unittest.mock import AsyncMock, MagicMock

from agents import ReceivingAgent
from broker import InMemoryEventBus
from database import WAREHOUSE_DB
from models import EventType, WarehouseEvent


class TestLiveModeTurnCompletion(unittest.IsolatedAsyncioTestCase):
    """Test suite validating that live LLM tool executions emit downstream events."""

    async def asyncSetUp(self) -> None:
        WAREHOUSE_DB.reset()
        self.bus = InMemoryEventBus()
        await self.bus.start()

        # Create mock OpenAI client
        self.mock_client = MagicMock()
        self.mock_client.chat.completions.create = AsyncMock()

        self.agent = ReceivingAgent(
            event_bus=self.bus,
            client=self.mock_client,
            mock_mode=False,  # Live mode
        )
        await self.agent.initialize()
        self.captured_events: list[tuple[str, WarehouseEvent]] = []

    async def asyncTearDown(self) -> None:
        await self.bus.stop()

    async def _capture_event(self, topic: str, event: WarehouseEvent) -> None:
        self.captured_events.append((topic, event))

    async def test_live_turn_tool_execution_emits_downstream_event(self) -> None:
        """
        When OpenAI model responds with a tool call to register_inbound_pallet
        followed by a final text response, on_turn_completed emits PALLET_RECEIVED.
        """
        await self.bus.subscribe("warehouse.inbound.pallet_received", self._capture_event)

        # 1. First completion response: model asks to call register_inbound_pallet
        tool_call = MagicMock()
        tool_call.id = "call_abc123"
        tool_call.function.name = "register_inbound_pallet"
        tool_call.function.arguments = json.dumps({
            "sku": "SKU-LIVE-TEST",
            "quantity": 50,
            "weight_kg": 200.0,
            "dock_id": "DOCK-1",
        })

        first_choice = MagicMock()
        first_choice.message.tool_calls = [tool_call]
        first_choice.message.content = None
        first_choice.message.to_dict.return_value = {
            "role": "assistant",
            "content": None,
            "tool_calls": [{
                "id": "call_abc123",
                "type": "function",
                "function": {
                    "name": "register_inbound_pallet",
                    "arguments": tool_call.function.arguments,
                },
            }],
        }
        first_response = MagicMock()
        first_response.choices = [first_choice]

        # 2. Second completion response: model returns final text summary (no tool calls)
        second_choice = MagicMock()
        second_choice.message.tool_calls = None
        second_choice.message.content = "Pallet registered at DOCK-1 successfully."
        second_choice.message.to_dict.return_value = {
            "role": "assistant",
            "content": "Pallet registered at DOCK-1 successfully.",
        }
        second_response = MagicMock()
        second_response.choices = [second_choice]

        # Sequence the mock client returns
        self.mock_client.chat.completions.create.side_effect = [
            first_response,
            second_response,
        ]

        # Execute turn in live mode
        res = await self.agent.execute_turn(
            user_prompt="Process arrival for SKU-LIVE-TEST",
            correlation_id="CORR-LIVE-1",
            context={"asn_id": "ASN-101"},
        )

        self.assertEqual(res, "Pallet registered at DOCK-1 successfully.")
        await self.bus.wait_until_idle()

        # Assert downstream event was emitted to broker
        self.assertEqual(len(self.captured_events), 1)
        topic, event = self.captured_events[0]
        self.assertEqual(topic, "warehouse.inbound.pallet_received")
        self.assertEqual(event.event_type, EventType.PALLET_RECEIVED)
        self.assertEqual(event.payload["sku"], "SKU-LIVE-TEST")
        self.assertEqual(event.payload["quantity"], 50)
        self.assertEqual(event.correlation_id, "CORR-LIVE-1")
        self.assertEqual(event.source_agent, "ReceivingAgent")


if __name__ == "__main__":
    unittest.main()
