"""
Warehouse Swarm Simulation - Unit Tests for Ledger Causal Tracing.

Validates that transactional state transitions in the warehouse ledger preserve
the upstream event's correlation_id across all operations.
"""

from __future__ import annotations

import unittest

from agents import PutawayAgent, ReceivingAgent, TransportAgent
from broker import InMemoryEventBus
from database import WAREHOUSE_DB
from models import EventType, WarehouseEvent
from tools import (
    complete_transport_mission,
    dispatch_transport_mission,
    pack_and_dispatch_order,
    quarantine_pallet,
    register_inbound_pallet,
    reserve_bin_for_putaway,
)


class TestLedgerCausalTracing(unittest.IsolatedAsyncioTestCase):
    """Test suite validating end-to-end correlation ID persistence in ledger entries."""

    async def asyncSetUp(self) -> None:
        WAREHOUSE_DB.reset()
        self.bus = InMemoryEventBus()
        await self.bus.start()

    async def asyncTearDown(self) -> None:
        await self.bus.stop()

    def test_direct_tools_persist_correlation_id_to_ledger(self) -> None:
        """Calling tools directly with correlation_id stamps ledger entries correctly."""
        corr_id = "CORR-TEST-INBOUND-99"

        # 1. Inbound receipt
        reg = register_inbound_pallet(
            sku="SKU-TRACE",
            quantity=100,
            weight_kg=300.0,
            correlation_id=corr_id,
        )
        pallet_id = reg["pallet_id"]

        self.assertEqual(len(WAREHOUSE_DB.ledger), 1)
        self.assertEqual(WAREHOUSE_DB.ledger[0].correlation_id, corr_id)

        # 2. Transport dispatch & completion
        bin_id = "BIN-A-1-1-1"
        reserve_bin_for_putaway(bin_id, pallet_id)
        msn = dispatch_transport_mission(
            pallet_id, "DOCK-1", bin_id, correlation_id=corr_id
        )
        complete_transport_mission(msn["mission_id"])

        self.assertEqual(len(WAREHOUSE_DB.ledger), 2)
        self.assertEqual(WAREHOUSE_DB.ledger[1].correlation_id, corr_id)

        # 3. Quarantine tool
        q_corr_id = "CORR-TEST-QA-100"
        quarantine_pallet(pallet_id, "Defective barcodes", correlation_id=q_corr_id)

        self.assertEqual(len(WAREHOUSE_DB.ledger), 3)
        self.assertEqual(WAREHOUSE_DB.ledger[2].correlation_id, q_corr_id)

    async def test_multi_agent_workflow_preserves_causal_chain_in_ledger(self) -> None:
        """
        When a shipment arrival event triggers Receiving -> Putaway -> Transport,
        all resulting ledger entries share the exact same correlation_id.
        """
        causal_correlation_id = "CORR-CHAIN-SHIPMENT-777"

        receiving_agent = ReceivingAgent(event_bus=self.bus, mock_mode=True)
        putaway_agent = PutawayAgent(event_bus=self.bus, mock_mode=True)
        transport_agent = TransportAgent(event_bus=self.bus, mock_mode=True)

        await receiving_agent.initialize()
        await putaway_agent.initialize()
        await transport_agent.initialize()

        arrival_event = WarehouseEvent(
            event_type=EventType.SHIPMENT_ARRIVED,
            payload={
                "asn_id": "ASN-101",
                "pallets": [
                    {"sku": "SKU-WIDGET-A", "qty": 100, "weight_kg": 350.0},
                ],
            },
            correlation_id=causal_correlation_id,
            source_agent="DOCK_SCANNER",
        )

        await self.bus.publish("warehouse.inbound.shipment_arrived", arrival_event)
        await self.bus.wait_until_idle()

        # Both the receipt and the transport completion must share causal_correlation_id
        entries = [
            e for e in WAREHOUSE_DB.ledger if e.correlation_id == causal_correlation_id
        ]
        self.assertGreaterEqual(len(entries), 2)
        reasons = [e.reason for e in entries]
        self.assertTrue(any("Inbound receipt" in r for r in reasons))
        self.assertTrue(any("Transport mission" in r for r in reasons))


if __name__ == "__main__":
    unittest.main()
