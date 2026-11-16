"""
Warehouse Swarm Simulation - Inventory Controller Agent.

Acts as the single source of audit truth across the warehouse.
Tracks stock levels, verifies ledger transactions following putaway or picking,
runs stock reconciliations, and emits replenishment warnings when stock falls below thresholds.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, Optional

from agents.base_agent import BaseAgent
from broker.base import EventBus
from models import EventType, WarehouseEvent
from tools import audit_ledger_history, get_inventory_summary

logger = logging.getLogger("warehouse.agent.inventory")

INVENTORY_SYSTEM_PROMPT = """You are the Inventory Controller Agent for an automated warehouse swarm.
Your duty is to maintain strict audit accuracy, inspect stock levels, and monitor the transaction ledger.

When an inventory event (transport completion, order dispatch, or audit request) arrives:
1. Call get_inventory_summary to inspect current quantities and locations across bins.
2. Call audit_ledger_history to verify recent transactions and state transitions.
3. Check if any SKU has fallen below the safety replenishment threshold (default: 20 units).
4. Report an audit summary with current available stock and any inventory alerts.
Always ensure physical counts and ledger entries are synchronized.
"""


class InventoryAgent(BaseAgent):
    """
    Inventory Controller Agent responsible for real-time stock balances and audit logs.
    """

    def __init__(
        self,
        event_bus: EventBus,
        model: str = "gpt-4o-mini",
        mock_mode: Optional[bool] = None,
        replenishment_threshold: int = 20,
    ) -> None:
        super().__init__(
            name="InventoryAgent",
            system_prompt=INVENTORY_SYSTEM_PROMPT,
            event_bus=event_bus,
            model=model,
            mock_mode=mock_mode,
        )
        self.replenishment_threshold = replenishment_threshold
        self._setup_tools()

    def _setup_tools(self) -> None:
        """Register deterministic inventory audit tools with OpenAI function-calling schema."""
        self.register_tool(
            name="get_inventory_summary",
            description="Returns aggregated inventory audit across all bins and docks, broken down by SKU.",
            parameters={
                "type": "object",
                "properties": {
                    "sku": {
                        "type": "string",
                        "description": "Optional SKU identifier to filter by (e.g. 'SKU-WIDGET-A')",
                    },
                },
            },
            handler=get_inventory_summary,
        )

        self.register_tool(
            name="audit_ledger_history",
            description="Returns recent chronological ledger entries for verifying inventory consistency.",
            parameters={
                "type": "object",
                "properties": {
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of recent ledger transactions to return (default 20)",
                    },
                },
            },
            handler=audit_ledger_history,
        )

    async def initialize(self) -> None:
        """Subscribe to events modifying stock locations or quantities."""
        await self.subscribe(
            "warehouse.transport.completed", self.handle_transport_completed
        )
        await self.subscribe(
            "warehouse.outbound.order_dispatched", self.handle_order_dispatched
        )
        await self.subscribe(
            "warehouse.inventory.audit_requested", self.handle_audit_requested
        )

    async def handle_transport_completed(self, topic: str, event: WarehouseEvent) -> None:
        """Invoked when an item arrives at its destination bin or dock."""
        payload = event.payload
        prompt = (
            f"Transport mission '{payload.get('mission_id')}' completed for pallet '{payload.get('pallet_id')}'.\n"
            f"Destination: '{payload.get('destination')}'.\n"
            f"Verify inventory summary and check ledger audit trail."
        )
        self._current_event_context = payload
        await self.execute_turn(prompt, correlation_id=event.correlation_id)

    async def handle_order_dispatched(self, topic: str, event: WarehouseEvent) -> None:
        """Invoked when an outbound order is packed and dispatched."""
        payload = event.payload
        prompt = (
            f"Order '{payload.get('order_id')}' dispatched.\n"
            f"Verify depleted inventory and check if any SKU requires replenishment."
        )
        self._current_event_context = payload
        await self.execute_turn(prompt, correlation_id=event.correlation_id)

    async def handle_audit_requested(self, topic: str, event: WarehouseEvent) -> None:
        """Invoked on scheduled or manual cycle count audits."""
        payload = event.payload
        sku = payload.get("sku")
        prompt = f"Audit requested for SKU '{sku or 'ALL'}'. Generate comprehensive inventory report."
        self._current_event_context = payload
        await self.execute_turn(prompt, correlation_id=event.correlation_id)

    async def mock_reasoning(
        self, prompt: str, correlation_id: Optional[str] = None
    ) -> str:
        """
        Deterministic mock reasoning loop verifying inventory summary and ledger consistency.
        """
        logger.info("[InventoryAgent] Mock reasoning: auditing stock and ledger...")

        inv = get_inventory_summary()
        ledger = audit_ledger_history(limit=5)

        summary_lines = ["Inventory Audit Report:"]
        for sku, data in inv.get("inventory", {}).items():
            avail = data.get("available_quantity", 0)
            total = data.get("total_quantity", 0)
            summary_lines.append(f"  - {sku}: Available={avail}, Total={total}")

            # Replenishment check
            if avail < self.replenishment_threshold:
                logger.warning(f"[InventoryAgent] Replenishment alert: {sku} available stock ({avail}) < threshold ({self.replenishment_threshold})")
                await self.publish(
                    "warehouse.inventory.replenishment_alert",
                    WarehouseEvent(
                        event_type=EventType.INVENTORY_MISMATCH,
                        payload={
                            "sku": sku,
                            "available_quantity": avail,
                            "threshold": self.replenishment_threshold,
                        },
                        correlation_id=correlation_id or "CORR-REPLENISH-1",
                        source_agent=self.name,
                    ),
                )

        summary_lines.append(f"Recent ledger transactions checked: {len(ledger)}")
        return "\n".join(summary_lines)
