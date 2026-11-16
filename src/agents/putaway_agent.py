"""
Warehouse Swarm Simulation - Putaway & Slotting Agent.

Listens for newly received pallets at inbound docks, determines optimal
storage slotting according to weight, velocity, and zone constraints,
reserves the candidate bin atomically, and requests transport dispatch.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, Optional

from agents.base_agent import BaseAgent
from broker.base import EventBus
from models import EventType, WarehouseEvent
from tools import find_candidate_bins, release_bin_reservation, reserve_bin_for_putaway

logger = logging.getLogger("warehouse.agent.putaway")

PUTAWAY_SYSTEM_PROMPT = """You are the Putaway & Slotting Agent for an automated warehouse swarm.
Your duty is to locate optimal storage rack bins for arriving pallets and reserve them.

When a pallet received notification arrives:
1. Examine the pallet's SKU, quantity, weight, and current dock location.
2. Determine preferred zone based on weight and velocity:
   - Light goods (< 500kg, fast turnover) -> Zone A
   - Medium goods (500kg - 1000kg) -> Zone B
   - Heavy cargo (> 1000kg) -> Zone C
3. Call find_candidate_bins to get available unblocked bins with sufficient weight capacity.
4. Choose the highest-scoring candidate bin and call reserve_bin_for_putaway.
5. If reservation succeeds, report the assigned bin and pallet details.
Always enforce physical constraints and lock the bin atomically via tools.
"""


class PutawayAgent(BaseAgent):
    """
    Putaway Agent managing slotting decisions and rack bin reservations.
    """

    def __init__(
        self,
        event_bus: EventBus,
        model: str = "gpt-4o-mini",
        mock_mode: Optional[bool] = None,
    ) -> None:
        super().__init__(
            name="PutawayAgent",
            system_prompt=PUTAWAY_SYSTEM_PROMPT,
            event_bus=event_bus,
            model=model,
            mock_mode=mock_mode,
        )
        self._setup_tools()

    def _setup_tools(self) -> None:
        """Register deterministic slotting tools with OpenAI function-calling schema."""
        self.register_tool(
            name="find_candidate_bins",
            description="Searches for empty, unblocked bins matching weight and zone criteria.",
            parameters={
                "type": "object",
                "properties": {
                    "weight_kg": {"type": "number", "description": "Pallet weight in kilograms"},
                    "preferred_zone": {
                        "type": "string",
                        "description": "Optional preferred zone ('A', 'B', or 'C')",
                    },
                },
                "required": ["weight_kg"],
            },
            handler=find_candidate_bins,
        )

        self.register_tool(
            name="reserve_bin_for_putaway",
            description="Atomically locks an empty bin from EMPTY to RESERVED_PUTAWAY.",
            parameters={
                "type": "object",
                "properties": {
                    "bin_id": {"type": "string", "description": "Bin identifier (e.g. 'BIN-A-1-1-1')"},
                    "pallet_id": {"type": "string", "description": "Identifier of the pallet being stored"},
                },
                "required": ["bin_id", "pallet_id"],
            },
            handler=reserve_bin_for_putaway,
        )

        self.register_tool(
            name="release_bin_reservation",
            description="Releases a reserved bin back to EMPTY if putaway is cancelled.",
            parameters={
                "type": "object",
                "properties": {
                    "bin_id": {"type": "string", "description": "Bin identifier to release"},
                },
                "required": ["bin_id"],
            },
            handler=release_bin_reservation,
        )

    async def initialize(self) -> None:
        """Subscribe to pallet received events from the Inbound stage."""
        await self.subscribe(
            "warehouse.inbound.pallet_received", self.handle_pallet_received
        )

    async def handle_pallet_received(self, topic: str, event: WarehouseEvent) -> None:
        """Event handler triggered when ReceivingAgent registers an intact pallet at a dock."""
        logger.info(
            f"[{self.name}] Pallet received event via topic '{topic}' "
            f"[correlation: {event.correlation_id}]"
        )
        payload = event.payload
        pallet_id = payload.get("pallet_id")
        sku = payload.get("sku")
        weight_kg = payload.get("weight_kg", 250.0)
        location = payload.get("location", "DOCK-1")

        prompt = (
            f"A new pallet '{pallet_id}' holding SKU '{sku}' (weight: {weight_kg}kg) is waiting at '{location}'.\n"
            f"Find an optimal candidate bin and reserve it for putaway."
        )

        # Store contextual metadata for mock_reasoning or LLM execution
        self._current_pallet_context = payload
        await self.execute_turn(prompt, correlation_id=event.correlation_id)

    async def mock_reasoning(
        self, prompt: str, correlation_id: Optional[str] = None
    ) -> str:
        """
        Deterministic mock reasoning loop mirroring the LLM slotting calculation.
        Queries candidate bins, reserves the optimal slot, and emits TRANSPORT_REQUESTED.
        """
        logger.info("[PutawayAgent] Mock reasoning: calculating slotting and reserving bin...")

        payload = getattr(self, "_current_pallet_context", {})
        pallet_id = payload.get("pallet_id", "PLT-UNKNOWN")
        sku = payload.get("sku", "SKU-UNKNOWN")
        weight_kg = payload.get("weight_kg", 300.0)
        source_dock = payload.get("location", "DOCK-1")

        # Determine zone heuristic
        if weight_kg > 1000.0:
            preferred_zone = "C"
        elif weight_kg > 500.0:
            preferred_zone = "B"
        else:
            preferred_zone = "A"

        candidates = find_candidate_bins(weight_kg=weight_kg, preferred_zone=preferred_zone)
        if not candidates:
            logger.error(f"[PutawayAgent] No candidate bins found for pallet {pallet_id} ({weight_kg}kg)!")
            return f"Failed: No bins available for weight {weight_kg}kg."

        target_bin = candidates[0]["bin_id"]
        res = reserve_bin_for_putaway(bin_id=target_bin, pallet_id=pallet_id)

        if not res.get("success"):
            logger.error(f"[PutawayAgent] Bin reservation failed: {res.get('error')}")
            return f"Reservation error: {res.get('error')}"

        logger.info(f"[PutawayAgent] Reserved bin '{target_bin}' for pallet '{pallet_id}'")

        # Emit transport mission request for downstream Fleet/Transport Agent
        await self.publish(
            "warehouse.transport.requested",
            WarehouseEvent(
                event_type=EventType.PUTAWAY_REQUESTED,
                payload={
                    "pallet_id": pallet_id,
                    "sku": sku,
                    "source": source_dock,
                    "destination": target_bin,
                    "weight_kg": weight_kg,
                },
                correlation_id=correlation_id or "CORR-PUTAWAY-1",
                source_agent=self.name,
            ),
        )

        return f"Successfully reserved bin {target_bin} for pallet {pallet_id} and requested transport."
