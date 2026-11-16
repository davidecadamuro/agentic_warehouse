"""
Warehouse Swarm Simulation - Inbound & Receiving Agent.

First line of agentic intake at the warehouse docks.
Verifies Advanced Shipping Notices (ASNs), inspects incoming pallets, registers
cargo into dock inventory, or quarantines damaged deliveries.
"""

from __future__ import annotations

import json
import logging
from typing import Optional

from agents.base_agent import BaseAgent
from broker.base import EventBus
from models import EventType, PalletCondition, WarehouseEvent
from tools import quarantine_pallet, register_inbound_pallet, verify_asn

logger = logging.getLogger("warehouse.agent.receiving")

RECEIVING_SYSTEM_PROMPT = """You are the Inbound & Receiving Agent for an automated warehouse swarm.
Your duty is to process arriving freight shipments at the inbound receiving docks.

When a shipment arrival notification is received:
1. Verify the Advance Shipping Notice (ASN) using verify_asn.
2. For each pallet/cargo item in the delivery:
   - If the pallet condition is DAMAGED or fails inspection, call quarantine_pallet to move it to QUARANTINE-BAY.
   - If the pallet is intact and matches the manifest, register it using register_inbound_pallet.
3. Provide a clear summary report of received vs quarantined pallets.
Always invoke the appropriate tools to record these actions deterministically in the warehouse database.
"""


class ReceivingAgent(BaseAgent):
    """
    Inbound Receiving Agent processing supplier deliveries and dock arrivals.
    """

    def __init__(
        self,
        event_bus: EventBus,
        model: str = "gpt-4o-mini",
        mock_mode: Optional[bool] = None,
    ) -> None:
        super().__init__(
            name="ReceivingAgent",
            system_prompt=RECEIVING_SYSTEM_PROMPT,
            event_bus=event_bus,
            model=model,
            mock_mode=mock_mode,
        )
        self._setup_tools()

    def _setup_tools(self) -> None:
        """Register deterministic tools with OpenAI function-calling schema."""
        self.register_tool(
            name="verify_asn",
            description="Verifies an Inbound Advance Shipping Notice (ASN) against expected manifests.",
            parameters={
                "type": "object",
                "properties": {
                    "asn_id": {"type": "string", "description": "The ASN identifier, e.g. 'ASN-101'"},
                },
                "required": ["asn_id"],
            },
            handler=verify_asn,
        )

        self.register_tool(
            name="register_inbound_pallet",
            description="Registers an intact pallet at the receiving dock, creating a unique pallet ID.",
            parameters={
                "type": "object",
                "properties": {
                    "sku": {"type": "string", "description": "The SKU code (e.g. 'SKU-WIDGET-A')"},
                    "quantity": {"type": "integer", "description": "Number of units on the pallet"},
                    "weight_kg": {"type": "number", "description": "Gross weight of pallet in kilograms"},
                    "dock_id": {"type": "string", "description": "Receiving dock name, default 'DOCK-1'"},
                },
                "required": ["sku", "quantity", "weight_kg"],
            },
            handler=lambda sku, quantity, weight_kg, dock_id="DOCK-1": register_inbound_pallet(
                sku=sku,
                quantity=quantity,
                weight_kg=weight_kg,
                dock_id=dock_id,
                condition=PalletCondition.INTACT,
            ),
        )

        self.register_tool(
            name="quarantine_pallet",
            description="Isolates a damaged or non-compliant pallet into the quarantine bay.",
            parameters={
                "type": "object",
                "properties": {
                    "pallet_id": {"type": "string", "description": "Identifier of the pallet to quarantine"},
                    "reason": {"type": "string", "description": "Explanation for quarantine"},
                },
                "required": ["pallet_id", "reason"],
            },
            handler=quarantine_pallet,
        )

    async def initialize(self) -> None:
        """Subscribe to shipment arrival events on the event bus."""
        await self.subscribe(
            "warehouse.inbound.shipment_arrived", self.handle_shipment_arrived
        )

    async def handle_shipment_arrived(self, topic: str, event: WarehouseEvent) -> None:
        """Event handler triggered when a truck arrives at the receiving dock."""
        logger.info(
            f"[{self.name}] Inbound shipment event triggered via topic '{topic}' "
            f"[correlation: {event.correlation_id}]"
        )
        payload = event.payload
        asn_id = payload.get("asn_id", "UNKNOWN")
        pallets = payload.get("pallets", [])

        prompt = (
            f"A shipment arrived at DOCK-1 under ASN '{asn_id}'.\n"
            f"Physical pallet items presented for unloading:\n{json.dumps(pallets, indent=2)}\n"
            f"Verify the ASN and register or quarantine each pallet accordingly."
        )

        await self.execute_turn(prompt, correlation_id=event.correlation_id)

    async def mock_reasoning(
        self, prompt: str, correlation_id: Optional[str] = None
    ) -> str:
        """
        Deterministic mock reasoning loop mirroring the LLM decision path.
        Extracts delivery data from the prompt or registered context and invokes tools.
        """
        logger.info("[ReceivingAgent] Mock reasoning: processing inbound shipment...")

        # In mock mode, we verify the ASN directly and process each pallet
        # We can extract details from the prompt or handle standard simulation payload
        summary = []
        if "DAMAGED" in prompt:
            reg_res = register_inbound_pallet(
                sku="SKU-FRAGILE",
                quantity=10,
                weight_kg=150.0,
                dock_id="DOCK-1",
                condition=PalletCondition.DAMAGED,
            )
            q_res = quarantine_pallet(reg_res["pallet_id"], "Damaged packaging upon arrival")
            summary.append(f"Quarantined: {q_res}")

            await self.publish(
                "warehouse.inbound.pallet_quarantined",
                WarehouseEvent(
                    event_type=EventType.PALLET_QUARANTINED,
                    payload=q_res,
                    correlation_id=correlation_id or "CORR-QUARANTINE-1",
                    source_agent=self.name,
                ),
            )
        elif "ASN-101" in prompt:
            asn_res = verify_asn("ASN-101")
            summary.append(f"ASN Verified: {asn_res}")

            # Register intact pallet
            reg_res = register_inbound_pallet(
                sku="SKU-WIDGET-A",
                quantity=100,
                weight_kg=350.0,
                dock_id="DOCK-1",
                condition=PalletCondition.INTACT,
            )
            summary.append(f"Registered: {reg_res['pallet_id']}")

            # Emit PALLET_RECEIVED event for downstream PutawayAgent
            await self.publish(
                "warehouse.inbound.pallet_received",
                WarehouseEvent(
                    event_type=EventType.PALLET_RECEIVED,
                    payload=reg_res,
                    correlation_id=correlation_id or "CORR-INBOUND-1",
                    source_agent=self.name,
                ),
            )
        else:
            # Generic fallback
            reg_res = register_inbound_pallet(
                sku="SKU-WIDGET-A",
                quantity=50,
                weight_kg=200.0,
                dock_id="DOCK-1",
            )
            await self.publish(
                "warehouse.inbound.pallet_received",
                WarehouseEvent(
                    event_type=EventType.PALLET_RECEIVED,
                    payload=reg_res,
                    correlation_id=correlation_id or "CORR-INBOUND-GEN",
                    source_agent=self.name,
                ),
            )
            summary.append(f"Registered generic pallet: {reg_res['pallet_id']}")

        return "\n".join(summary)
