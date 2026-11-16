"""
Warehouse Swarm Simulation - Exception & Anomaly Handling Agent.

Monitors anomaly events across the warehouse (blocked aisles, vehicle stalls,
inventory discrepancies, damaged goods). Enforces corridor safety locks,
re-routes affected agents, or notifies human operators.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from agents.base_agent import BaseAgent
from broker.base import EventBus
from models import EventType, WarehouseEvent
from tools import block_corridor_zone, unblock_corridor_zone

logger = logging.getLogger("warehouse.agent.exception")

EXCEPTION_SYSTEM_PROMPT = """You are the Exception & Escalation Agent for an automated warehouse swarm.
Your duty is to mitigate disruptions, cordon off dangerous zones, and recover stalled workflows.

When an anomaly event (obstacle, vehicle breakdown, or inventory mismatch) is reported:
1. Examine the affected zone, vehicle, or cargo identifier.
2. If an obstacle or spillage is reported, call block_corridor_zone to cordon off the aisle.
3. If an area has been cleared, call unblock_corridor_zone to restore traffic.
4. Log an incident report and trigger downstream compensatory events.
Always prioritize warehouse safety and automated rerouting.
"""


class ExceptionAgent(BaseAgent):
    """
    Exception & Anomaly Agent managing safety locks and workflow recovery.
    """

    def __init__(
        self,
        event_bus: EventBus,
        model: str = "gpt-4o-mini",
        mock_mode: Optional[bool] = None,
    ) -> None:
        super().__init__(
            name="ExceptionAgent",
            system_prompt=EXCEPTION_SYSTEM_PROMPT,
            event_bus=event_bus,
            model=model,
            mock_mode=mock_mode,
        )
        self.incident_log: list[Dict[str, Any]] = []
        self._setup_tools()

    def _setup_tools(self) -> None:
        """Register deterministic safety tools with OpenAI function-calling schema."""
        self.register_tool(
            name="block_corridor_zone",
            description="Cordon off a warehouse zone due to obstacle, spillage, or breakdown.",
            parameters={
                "type": "object",
                "properties": {
                    "zone_id": {"type": "string", "description": "Zone code to block ('A', 'B', or 'C')"},
                    "reason": {"type": "string", "description": "Incident explanation"},
                },
                "required": ["zone_id", "reason"],
            },
            handler=block_corridor_zone,
        )

        self.register_tool(
            name="unblock_corridor_zone",
            description="Restores corridor access after clearance.",
            parameters={
                "type": "object",
                "properties": {
                    "zone_id": {"type": "string", "description": "Zone code to unblock"},
                },
                "required": ["zone_id"],
            },
            handler=unblock_corridor_zone,
        )

    async def initialize(self) -> None:
        """Subscribe to exception and anomaly topics."""
        await self.subscribe(
            "warehouse.exception.obstacle_reported", self.handle_obstacle_reported
        )
        await self.subscribe(
            "warehouse.exception.obstacle_cleared", self.handle_obstacle_cleared
        )
        await self.subscribe(
            "warehouse.inbound.pallet_quarantined", self.handle_pallet_quarantined
        )

    async def handle_obstacle_reported(self, topic: str, event: WarehouseEvent) -> None:
        """Invoked when a sensor or AGV reports a blocked corridor."""
        payload = event.payload
        zone_id = payload.get("zone_id", "A")
        reason = payload.get("reason", "Obstacle detected")

        prompt = (
            f"Obstacle reported in Zone '{zone_id}': {reason}.\n"
            f"Lock corridor zone to prevent collisions and reroute other agents."
        )
        self._current_exception_context = payload
        await self.execute_turn(prompt, correlation_id=event.correlation_id)

    async def handle_obstacle_cleared(self, topic: str, event: WarehouseEvent) -> None:
        """Invoked when maintenance clears a corridor."""
        payload = event.payload
        zone_id = payload.get("zone_id", "A")

        prompt = f"Zone '{zone_id}' has been cleared by maintenance. Restore access."
        self._current_exception_context = payload
        await self.execute_turn(prompt, correlation_id=event.correlation_id)

    async def handle_pallet_quarantined(self, topic: str, event: WarehouseEvent) -> None:
        """Invoked when ReceivingAgent flags damaged goods."""
        payload = event.payload
        logger.warning(f"[{self.name}] Incident logged: Pallet '{payload.get('pallet_id')}' quarantined ({payload.get('reason')}).")
        self.incident_log.append({
            "type": "DAMAGED_CARGO",
            "pallet_id": payload.get("pallet_id"),
            "reason": payload.get("reason"),
            "timestamp": event.timestamp.isoformat(),
        })

    async def mock_reasoning(
        self, prompt: str, correlation_id: Optional[str] = None
    ) -> str:
        """
        Deterministic mock reasoning loop executing corridor locks and logging incidents.
        """
        logger.info("[ExceptionAgent] Mock reasoning: managing corridor safety...")

        payload = getattr(self, "_current_exception_context", {})
        zone_id = payload.get("zone_id", "A")
        reason = payload.get("reason", "Reported anomaly")

        if "cleared" in prompt.lower() or "restore" in prompt.lower():
            res = unblock_corridor_zone(zone_id)
            logger.info(f"[ExceptionAgent] Restored Zone '{zone_id}' access.")
            await self.publish(
                "warehouse.corridor.unblocked",
                WarehouseEvent(
                    event_type=EventType.CORRIDOR_UNBLOCKED,
                    payload={"zone_id": zone_id},
                    correlation_id=correlation_id or "CORR-UNBLOCK-1",
                    source_agent=self.name,
                ),
            )
            return f"Zone {zone_id} unblocked and restored."

        # Otherwise block zone
        res = block_corridor_zone(zone_id, reason)
        self.incident_log.append({
            "type": "ZONE_BLOCK",
            "zone_id": zone_id,
            "reason": reason,
        })
        logger.warning(f"[ExceptionAgent] Blocked Zone '{zone_id}': {reason}")

        await self.publish(
            "warehouse.corridor.blocked",
            WarehouseEvent(
                event_type=EventType.CORRIDOR_BLOCKED,
                payload={"zone_id": zone_id, "reason": reason},
                correlation_id=correlation_id or "CORR-BLOCK-1",
                source_agent=self.name,
            ),
        )

        return f"Zone {zone_id} blocked. Reason: {reason}."
