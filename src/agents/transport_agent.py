"""
Warehouse Swarm Simulation - Fleet / Transport Agent.

Responsible for autonomous mobile robot (AGV / AMR / Forklift) fleet dispatching.
Listens for transport requests (putaway or pick transfers), queries vehicle availability,
matches payload capacities and battery levels, dispatches missions, and marks
them complete upon arrival.
"""

from __future__ import annotations

import logging
from typing import Optional

from agents.base_agent import BaseAgent
from broker.base import EventBus
from models import EventType, WarehouseEvent
from tools import (
    complete_transport_mission,
    dispatch_transport_mission,
    query_available_vehicles,
)

logger = logging.getLogger("warehouse.agent.transport")

TRANSPORT_SYSTEM_PROMPT = """You are the Fleet & Transport Agent for an automated warehouse swarm.
Your duty is to dispatch and monitor autonomous vehicles (AGVs, AMRs, Forklifts) moving cargo.

When a transport request is received:
1. Examine the pallet ID, gross weight, source location, and destination location.
2. Call query_available_vehicles with min_capacity_kg matching the pallet weight.
3. Select an optimal vehicle with sufficient battery level (>= 20%) and payload capacity.
4. Call dispatch_transport_mission to assign the mission and transition the vehicle to BUSY.
5. In simulated execution, once transport transit completes, call complete_transport_mission.
6. Report the completed movement and updated pallet position.
Always ensure physical vehicle capacity and battery constraints are respected.
"""


class TransportAgent(BaseAgent):
    """
    Fleet & Transport Agent managing autonomous vehicle missions.
    """

    def __init__(
        self,
        event_bus: EventBus,
        model: str = "gpt-4o-mini",
        mock_mode: Optional[bool] = None,
    ) -> None:
        super().__init__(
            name="TransportAgent",
            system_prompt=TRANSPORT_SYSTEM_PROMPT,
            event_bus=event_bus,
            model=model,
            mock_mode=mock_mode,
        )
        self._setup_tools()

    def _setup_tools(self) -> None:
        """Register deterministic fleet dispatch tools with OpenAI function-calling schema."""
        self.register_tool(
            name="query_available_vehicles",
            description="Returns available transport vehicles with battery >= 20% and adequate capacity.",
            parameters={
                "type": "object",
                "properties": {
                    "min_capacity_kg": {
                        "type": "number",
                        "description": "Minimum payload capacity required in kilograms",
                    },
                },
                "required": ["min_capacity_kg"],
            },
            handler=query_available_vehicles,
        )

        self.register_tool(
            name="dispatch_transport_mission",
            description="Assigns a vehicle to transport a pallet from source to destination.",
            parameters={
                "type": "object",
                "properties": {
                    "pallet_id": {"type": "string", "description": "Pallet identifier to move"},
                    "source": {"type": "string", "description": "Pickup location (dock or bin)"},
                    "destination": {"type": "string", "description": "Dropoff location (bin or bay)"},
                    "preferred_vehicle_id": {
                        "type": "string",
                        "description": "Optional specific vehicle ID (e.g. 'AGV-01')",
                    },
                },
                "required": ["pallet_id", "source", "destination"],
            },
            handler=dispatch_transport_mission,
        )

        self.register_tool(
            name="complete_transport_mission",
            description="Completes an active transport mission, updating physical locations and freeing vehicle.",
            parameters={
                "type": "object",
                "properties": {
                    "mission_id": {"type": "string", "description": "Active mission ID to mark complete"},
                },
                "required": ["mission_id"],
            },
            handler=complete_transport_mission,
        )

    async def initialize(self) -> None:
        """Subscribe to transport request topics on the event bus."""
        await self.subscribe(
            "warehouse.transport.requested", self.handle_transport_requested
        )

    async def handle_transport_requested(self, topic: str, event: WarehouseEvent) -> None:
        """Event handler triggered when PutawayAgent or OrderFulfillmentAgent requests cargo movement."""
        logger.info(
            f"[{self.name}] Transport requested event via topic '{topic}' "
            f"[correlation: {event.correlation_id}]"
        )
        payload = event.payload
        pallet_id = payload.get("pallet_id")
        source = payload.get("source")
        destination = payload.get("destination")
        weight_kg = payload.get("weight_kg", 300.0)

        prompt = (
            f"Transport mission requested for pallet '{pallet_id}' (weight: {weight_kg}kg).\n"
            f"Route: '{source}' -> '{destination}'.\n"
            f"Query available vehicles, dispatch mission, and confirm arrival."
        )

        self._current_transport_context = payload
        await self.execute_turn(prompt, correlation_id=event.correlation_id)

    async def mock_reasoning(
        self, prompt: str, correlation_id: Optional[str] = None
    ) -> str:
        """
        Deterministic mock reasoning loop mirroring the LLM fleet dispatch.
        Queries available AGVs/Forklifts, dispatches mission, marks complete, and emits TRANSPORT_COMPLETED.
        """
        logger.info("[TransportAgent] Mock reasoning: dispatching vehicle mission...")

        payload = getattr(self, "_current_transport_context", {})
        pallet_id = payload.get("pallet_id", "PLT-UNKNOWN")
        source = payload.get("source", "DOCK-1")
        destination = payload.get("destination", "UNKNOWN")
        weight_kg = payload.get("weight_kg", 300.0)

        # 1. Query available vehicles
        vehicles = query_available_vehicles(min_capacity_kg=weight_kg)
        if not vehicles:
            logger.error(f"[TransportAgent] No available vehicle with capacity >= {weight_kg}kg!")
            return f"Failed: No vehicle available with payload capacity {weight_kg}kg."

        chosen_vehicle_id = vehicles[0]["vehicle_id"]

        # 2. Dispatch mission
        dispatch_res = dispatch_transport_mission(
            pallet_id=pallet_id,
            source=source,
            destination=destination,
            preferred_vehicle_id=chosen_vehicle_id,
        )

        if not dispatch_res.get("success"):
            logger.error(f"[TransportAgent] Dispatch failed: {dispatch_res.get('error')}")
            return f"Dispatch error: {dispatch_res.get('error')}"

        mission_id = dispatch_res["mission_id"]
        logger.info(f"[TransportAgent] Dispatched mission '{mission_id}' using vehicle '{chosen_vehicle_id}'")

        # 3. Simulate arrival and completion
        complete_res = complete_transport_mission(mission_id=mission_id)
        if not complete_res.get("success"):
            logger.error(f"[TransportAgent] Mission completion failed: {complete_res.get('error')}")
            return f"Completion error: {complete_res.get('error')}"

        logger.info(f"[TransportAgent] Completed mission '{mission_id}'. Pallet at '{destination}'.")

        # 4. Emit event for downstream InventoryAgent and OrderAgent
        await self.publish(
            "warehouse.transport.completed",
            WarehouseEvent(
                event_type=EventType.TRANSPORT_COMPLETED,
                payload={
                    "mission_id": mission_id,
                    "pallet_id": pallet_id,
                    "vehicle_id": chosen_vehicle_id,
                    "source": source,
                    "destination": destination,
                },
                correlation_id=correlation_id or "CORR-TRANSPORT-1",
                source_agent=self.name,
            ),
        )

        return (
            f"Successfully executed mission {mission_id} with vehicle {chosen_vehicle_id}. "
            f"Pallet {pallet_id} moved to {destination}."
        )
