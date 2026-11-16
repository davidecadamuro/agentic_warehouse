"""
Warehouse Swarm Simulation - Order Fulfillment & Picking Agent.

Processes customer orders, reserves matching stock across occupied bins,
dispatches picking transport missions from racks to packing bays, and completes
order dispatch once items arrive at the outbound dock.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from openai import AsyncOpenAI

from agents.base_agent import BaseAgent
from broker.base import EventBus
from models import EventType, WarehouseEvent
from tools import check_and_reserve_stock, pack_and_dispatch_order

logger = logging.getLogger("warehouse.agent.order")

ORDER_SYSTEM_PROMPT = """You are the Order Fulfillment & Picking Agent for an automated warehouse swarm.
Your duty is to manage outbound customer orders, reserve stock, and orchestrate picking tasks.

When an order submitted event is received:
1. Parse the order ID, SKU, and requested quantity.
2. Call check_and_reserve_stock to allocate inventory and transition bins to RESERVED_PICK.
3. If stock reservation succeeds, trigger transport missions to pick items from the allocated bins to PACKING-BAY-1.
4. Once picking transit completes, call pack_and_dispatch_order to deplete inventory and manifest dispatch.
5. Publish appropriate progress events.
Always prevent double-allocation by respecting atomic tool locks.
"""


class OrderFulfillmentAgent(BaseAgent):
    """
    Order Fulfillment & Picking Agent managing outbound order lifecycles.
    """

    def __init__(
        self,
        event_bus: EventBus,
        model: str = "gpt-4o-mini",
        client: Optional[AsyncOpenAI] = None,
        mock_mode: Optional[bool] = None,
    ) -> None:
        super().__init__(
            name="OrderFulfillmentAgent",
            system_prompt=ORDER_SYSTEM_PROMPT,
            event_bus=event_bus,
            model=model,
            client=client,
            mock_mode=mock_mode,
        )
        self._pending_orders: Dict[str, Dict[str, Any]] = {}
        self._setup_tools()

    def _setup_tools(self) -> None:
        """Register deterministic picking & dispatch tools with OpenAI function-calling schema."""
        self.register_tool(
            name="check_and_reserve_stock",
            description="Atomically reserves available stock for an order from occupied bins.",
            parameters={
                "type": "object",
                "properties": {
                    "sku": {"type": "string", "description": "SKU identifier requested"},
                    "quantity": {"type": "integer", "description": "Number of units to reserve"},
                    "order_id": {"type": "string", "description": "Customer order identifier"},
                },
                "required": ["sku", "quantity", "order_id"],
            },
            handler=check_and_reserve_stock,
        )

        self.register_tool(
            name="pack_and_dispatch_order",
            description="Completes packing, decrements inventory from allocations, and manifests dispatch.",
            parameters={
                "type": "object",
                "properties": {
                    "order_id": {"type": "string", "description": "Order identifier to pack and dispatch"},
                    "dispatch_dock": {
                        "type": "string",
                        "description": "Outbound dock identifier, default 'OUTBOUND-DOCK-1'",
                    },
                },
                "required": ["order_id"],
            },
            handler=pack_and_dispatch_order,
        )

    async def initialize(self) -> None:
        """Subscribe to order submissions and transport completion events."""
        await self.subscribe(
            "warehouse.outbound.order_submitted", self.handle_order_submitted
        )
        await self.subscribe(
            "warehouse.transport.completed", self.handle_transport_completed
        )

    async def handle_order_submitted(self, topic: str, event: WarehouseEvent) -> None:
        """Event handler triggered when a customer order is placed."""
        logger.info(
            f"[{self.name}] Order submitted via topic '{topic}' "
            f"[correlation: {event.correlation_id}]"
        )
        payload = event.payload
        order_id = payload.get("order_id", "ORD-UNKNOWN")
        sku = payload.get("sku", "SKU-UNKNOWN")
        quantity = payload.get("quantity", 1)

        self._pending_orders[order_id] = payload

        prompt = (
            f"Customer order '{order_id}' submitted for {quantity} units of '{sku}'.\n"
            f"Check availability and reserve inventory."
        )

        await self.execute_turn(prompt, correlation_id=event.correlation_id, context=payload)

    async def handle_transport_completed(self, topic: str, event: WarehouseEvent) -> None:
        """Invoked when an AGV completes delivery of picked items to PACKING-BAY-1."""
        payload = event.payload
        destination = payload.get("destination", "")

        # If item arrived at packing bay for picking
        if destination == "PACKING-BAY-1":
            pallet_id = payload.get("pallet_id")
            logger.info(f"[{self.name}] Pallet '{pallet_id}' arrived at PACKING-BAY-1. Proceeding to pack.")

            # Identify matching order strictly by exact pallet_id or allocated_pallets
            matched_order_id = None
            for ord_id, data in self._pending_orders.items():
                allocated_pids = data.get("allocated_pallet_ids", [])
                if data.get("pallet_id") == pallet_id or pallet_id in allocated_pids:
                    matched_order_id = ord_id
                    break

            if matched_order_id:
                dispatch_res = pack_and_dispatch_order(
                    matched_order_id,
                    correlation_id=event.correlation_id,
                )
                if dispatch_res.get("success"):
                    self._pending_orders.pop(matched_order_id, None)
                    await self.publish(
                        "warehouse.outbound.order_dispatched",
                        WarehouseEvent(
                            event_type=EventType.ORDER_DISPATCHED,
                            payload=dispatch_res,
                            correlation_id=event.correlation_id,
                            source_agent=self.name,
                        ),
                    )

                    # Trigger return transport for any residual stock on partially picked pallets
                    for residual in dispatch_res.get("residual_pallets", []):
                        r_pallet_id = residual["pallet_id"]
                        return_bin = residual["return_bin_id"]
                        logger.info(
                            f"[{self.name}] Residual stock detected ({residual['remaining_quantity']} units) "
                            f"on pallet '{r_pallet_id}'. Dispatching return transport to '{return_bin}'."
                        )
                        await self.publish(
                            "warehouse.transport.requested",
                            WarehouseEvent(
                                event_type=EventType.PUTAWAY_REQUESTED,
                                payload={
                                    "pallet_id": r_pallet_id,
                                    "sku": residual.get("sku", "SKU-UNKNOWN"),
                                    "source": "PACKING-BAY-1",
                                    "destination": return_bin,
                                    "weight_kg": residual.get("weight_kg", 200.0),
                                },
                                correlation_id=event.correlation_id,
                                source_agent=self.name,
                            ),
                        )

    async def on_turn_completed(
        self,
        turn_response: str,
        correlation_id: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
        executed_tools: Optional[list[Dict[str, Any]]] = None,
    ) -> None:
        """Emits pick transport requests when stock reservation succeeds in live mode."""
        if not executed_tools or not context:
            return

        order_id = context.get("order_id", "ORD-UNKNOWN")
        for tool in executed_tools:
            if tool.get("tool_name") == "check_and_reserve_stock":
                result = tool.get("result", {})
                if result.get("success"):
                    allocations = result.get("allocations", [])
                    allocated_pids = [alloc.get("pallet_id") for alloc in allocations if alloc.get("pallet_id")]
                    if order_id in self._pending_orders:
                        self._pending_orders[order_id]["allocated_pallet_ids"] = allocated_pids
                        self._pending_orders[order_id]["status"] = "RESERVED"

                    for alloc in allocations:
                        bin_id = alloc.get("bin_id")
                        pallet_id = alloc.get("pallet_id")

                        if order_id in self._pending_orders:
                            self._pending_orders[order_id]["pallet_id"] = pallet_id

                        await self.publish(
                            "warehouse.transport.requested",
                            WarehouseEvent(
                                event_type=EventType.PICK_DISPATCHED,
                                payload={
                                    "order_id": order_id,
                                    "pallet_id": pallet_id,
                                    "source": bin_id,
                                    "destination": "PACKING-BAY-1",
                                    "weight_kg": 200.0,
                                },
                                correlation_id=correlation_id or "CORR-PICK-LIVE",
                                source_agent=self.name,
                            ),
                        )

    async def mock_reasoning(
        self,
        prompt: str,
        correlation_id: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Deterministic mock reasoning loop reserving stock and initiating pick missions.
        """
        logger.info("[OrderFulfillmentAgent] Mock reasoning: checking stock and allocating...")

        payload = context or {}
        order_id = payload.get("order_id", "ORD-1")
        sku = payload.get("sku", "SKU-WIDGET-A")
        quantity = payload.get("quantity", 10)

        reserve_res = check_and_reserve_stock(
            sku=sku,
            quantity=quantity,
            order_id=order_id,
            correlation_id=correlation_id,
        )
        if not reserve_res.get("success"):
            err = reserve_res.get("error", "Insufficient stock")
            logger.warning(f"[OrderFulfillmentAgent] Stock allocation failed for {order_id}: {err}")
            return f"Failed: {err}"

        allocations = reserve_res["allocations"]
        logger.info(f"[OrderFulfillmentAgent] Successfully reserved {quantity} of {sku} across {len(allocations)} bin(s).")

        allocated_pids = [alloc["pallet_id"] for alloc in allocations]
        if order_id in self._pending_orders:
            self._pending_orders[order_id]["allocated_pallet_ids"] = allocated_pids
            self._pending_orders[order_id]["status"] = "RESERVED"

        # Emit pick transport request for each allocated bin to PACKING-BAY-1
        for alloc in allocations:
            bin_id = alloc["bin_id"]
            pallet_id = alloc["pallet_id"]

            if order_id in self._pending_orders:
                self._pending_orders[order_id]["pallet_id"] = pallet_id

            await self.publish(
                "warehouse.transport.requested",
                WarehouseEvent(
                    event_type=EventType.PICK_DISPATCHED,
                    payload={
                        "order_id": order_id,
                        "pallet_id": pallet_id,
                        "source": bin_id,
                        "destination": "PACKING-BAY-1",
                        "weight_kg": 200.0,
                    },
                    correlation_id=correlation_id or "CORR-PICK-1",
                    source_agent=self.name,
                ),
            )

        return f"Order {order_id} allocated and pick transport requested."
