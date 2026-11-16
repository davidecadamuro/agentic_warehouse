"""
Warehouse Swarm Simulation - End-to-End Working Day Scenario Runner.

Runs a concurrent multi-agent simulation demonstrating:
1. Inbound freight arrival and quarantine triage (ReceivingAgent).
2. Autonomous slotting and rack reservation (PutawayAgent).
3. AGV / AMR fleet transport from dock to rack (TransportAgent).
4. Outbound customer order intake, stock reservation, and pick wave (OrderFulfillmentAgent).
5. Continuous stock audit and replenishment monitoring (InventoryAgent).
6. Corridor obstacle containment and dynamic rerouting (ExceptionAgent).
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
from typing import Optional

from agents import (
    ExceptionAgent,
    InventoryAgent,
    OrderFulfillmentAgent,
    PutawayAgent,
    ReceivingAgent,
    TransportAgent,
)
from broker import InMemoryEventBus
from database import WAREHOUSE_DB
from models import EventType, WarehouseEvent

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
)
logger = logging.getLogger("warehouse.simulation")


def resolve_mock_mode(mock_mode: Optional[bool] = None) -> bool:
    if mock_mode is not None:
        return bool(mock_mode)

    env_value = os.getenv("WAREHOUSE_MOCK_MODE")
    if env_value is not None:
        if env_value == "True":
            return True
        if env_value == "False":
            return False
        raise ValueError("WAREHOUSE_MOCK_MODE must be set to either 'True' or 'False'.")

    return True


def resolve_model(model: Optional[str] = None) -> str:
    if model is not None:
        return model
    return os.getenv("OPENAI_MODEL", "gpt-6-luna")


async def run_simulation(mock_mode: Optional[bool] = None, model: Optional[str] = None) -> None:
    resolved_mock_mode = resolve_mock_mode(mock_mode)
    resolved_model = resolve_model(model)
    print(f"\n[CONFIG] mock_mode={resolved_mock_mode}, model={resolved_model}")
    print("\n" + "=" * 80)
    print("      WAREHOUSE AGENTIC SWARM SIMULATION - COMMENCING DAY SHIFT")
    print("=" * 80 + "\n")

    # 1. Reset database & initialize broker
    WAREHOUSE_DB.reset()
    bus = InMemoryEventBus()
    await bus.start()

    # 2. Instantiate and initialize all 6 agents
    receiving_agent = ReceivingAgent(event_bus=bus, mock_mode=resolved_mock_mode, model=resolved_model)
    putaway_agent = PutawayAgent(event_bus=bus, mock_mode=resolved_mock_mode, model=resolved_model)
    transport_agent = TransportAgent(event_bus=bus, mock_mode=resolved_mock_mode, model=resolved_model)
    inventory_agent = InventoryAgent(event_bus=bus, mock_mode=resolved_mock_mode, model=resolved_model, replenishment_threshold=25)
    order_agent = OrderFulfillmentAgent(event_bus=bus, mock_mode=resolved_mock_mode, model=resolved_model)
    exception_agent = ExceptionAgent(event_bus=bus, mock_mode=resolved_mock_mode, model=resolved_model)

    agents = [
        receiving_agent,
        putaway_agent,
        transport_agent,
        inventory_agent,
        order_agent,
        exception_agent,
    ]

    for agent in agents:
        await agent.initialize()

    print(">>> All 6 Swarm Agents initialized and connected to EventBus.")

    # --------------------------------------------------------------------------
    # SCENARIO STEP 1: Inbound Shipment Arrival (Intact Cargo + Damaged Cargo)
    # --------------------------------------------------------------------------
    print("\n[STEP 1] Arriving Truck at DOCK-1 with ASN-101...")
    await bus.publish(
        "warehouse.inbound.shipment_arrived",
        WarehouseEvent(
            event_type=EventType.SHIPMENT_ARRIVED,
            payload={
                "asn_id": "ASN-101",
                "pallets": [
                    {"sku": "SKU-WIDGET-A", "qty": 100, "weight_kg": 350.0},
                ],
            },
            source_agent="DOCK_SCANNER",
        ),
    )
    await bus.wait_until_idle()

    # Also simulate a damaged delivery
    print("\n[STEP 1.1] Arriving Damaged Pallet at DOCK-1...")
    await bus.publish(
        "warehouse.inbound.shipment_arrived",
        WarehouseEvent(
            event_type=EventType.SHIPMENT_ARRIVED,
            payload={
                "asn_id": "ASN-101",
                "pallets": [
                    {"sku": "SKU-FRAGILE", "qty": 10, "weight_kg": 150.0, "condition": "DAMAGED"},
                ],
            },
            source_agent="DOCK_SCANNER",
        ),
    )
    await bus.wait_until_idle()

    # --------------------------------------------------------------------------
    # SCENARIO STEP 2: Anomaly / Corridor Obstacle & Dynamic Rerouting
    # --------------------------------------------------------------------------
    print("\n[STEP 2] Simulating Obstacle in Zone A (AGV breakdown)...")
    await bus.publish(
        "warehouse.exception.obstacle_reported",
        WarehouseEvent(
            event_type=EventType.CORRIDOR_BLOCKED,
            payload={"zone_id": "A", "reason": "Coolant spill near Aisle 1"},
            source_agent="CORRIDOR_SENSOR",
        ),
    )
    await bus.wait_until_idle()

    print("\n[STEP 2.1] Incoming Pallet arrives while Zone A is blocked (Testing Detour)...")
    await bus.publish(
        "warehouse.inbound.shipment_arrived",
        WarehouseEvent(
            event_type=EventType.SHIPMENT_ARRIVED,
            payload={
                "asn_id": "ASN-101",
                "pallets": [
                    {"sku": "SKU-WIDGET-A", "qty": 50, "weight_kg": 400.0},
                ],
            },
            source_agent="DOCK_SCANNER",
        ),
    )
    await bus.wait_until_idle()

    # Restore Zone A
    print("\n[STEP 2.2] Maintenance clears Zone A...")
    await bus.publish(
        "warehouse.exception.obstacle_cleared",
        WarehouseEvent(
            event_type=EventType.CORRIDOR_UNBLOCKED,
            payload={"zone_id": "A"},
            source_agent="FACILITY_CREW",
        ),
    )
    await bus.wait_until_idle()

    # --------------------------------------------------------------------------
    # SCENARIO STEP 3: Outbound Customer Order Fulfillment & Picking
    # --------------------------------------------------------------------------
    print("\n[STEP 3] Customer Order ORD-9901 Placed for 30 units of SKU-WIDGET-A...")
    await bus.publish(
        "warehouse.outbound.order_submitted",
        WarehouseEvent(
            event_type=EventType.ORDER_SUBMITTED,
            payload={"order_id": "ORD-9901", "sku": "SKU-WIDGET-A", "quantity": 30},
            source_agent="ECOMMERCE_WEBHOOK",
        ),
    )
    await bus.wait_until_idle()

    # --------------------------------------------------------------------------
    # SCENARIO STEP 4: Inventory Reconciliation & End of Shift Audit
    # --------------------------------------------------------------------------
    print("\n[STEP 4] Triggering End-of-Shift Inventory Audit...")
    await bus.publish(
        "warehouse.inventory.audit_requested",
        WarehouseEvent(
            event_type=EventType.INVENTORY_MISMATCH,
            payload={"sku": "SKU-WIDGET-A"},
            source_agent="SHIFT_SUPERVISOR",
        ),
    )
    await bus.wait_until_idle()

    # Shutdown broker
    await bus.stop()

    print("\n" + "=" * 80)
    print("                    SIMULATION RESULTS & AUDIT TRAIL")
    print("=" * 80)

    # Print final ledger records
    print("\nRecent Ledger Transactions (with Causal Trace):")
    for entry in WAREHOUSE_DB.ledger:
        corr = entry.correlation_id[:13] + ".." if entry.correlation_id else "N/A          "
        print(f"  [{entry.timestamp.strftime('%H:%M:%S')}] [corr: {corr:15}] {entry.sku:16} (qty: {entry.quantity:3}) | {entry.source:15} -> {entry.destination:15} | {entry.reason}")

    # Print bin occupancy status
    occupied_bins = [b for b in WAREHOUSE_DB.bins.values() if b.status != "EMPTY"]
    print(f"\nActive Bins Occupied/Reserved: {len(occupied_bins)}")
    for b in occupied_bins:
        print(f"  {b.bin_id:18} | Status: {b.status.value:18} | Pallet: {b.current_pallet_id or b.reserved_by}")

    # Print fleet status
    print("\nTransport Fleet Status:")
    for v in WAREHOUSE_DB.vehicles.values():
        print(f"  {v.vehicle_id:12} | Status: {v.status.value:10} | Battery: {v.battery_level:.1f}% | Location: {v.current_location}")

    print("\n" + "=" * 80)
    print("               WAREHOUSE SWARM SIMULATION COMPLETED CLEANLY")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the warehouse swarm simulation.")
    parser.add_argument(
        "--mock-mode",
        choices=("true", "false"),
        help="Set mock_mode explicitly to 'true' or 'false'. If omitted, it falls back to WAREHOUSE_MOCK_MODE or OPENAI_API_KEY.",
    )
    parser.add_argument(
        "--model",
        help="OpenAI model to use for agent reasoning. Defaults to OPENAI_MODEL or gpt-4o-mini.",
    )
    args = parser.parse_args()
    mock_mode = None if args.mock_mode is None else args.mock_mode.lower() == "true"
    asyncio.run(run_simulation(mock_mode=mock_mode, model=args.model))
