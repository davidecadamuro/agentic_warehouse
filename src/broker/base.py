"""
Warehouse Swarm Simulation - Event Broker Abstract Interface.

Defines the contract for publish-subscribe event exchanges in the agentic swarm.
Any concrete broker implementation (In-Memory, RabbitMQ, Kafka, Redis Streams)
must adhere to this abstract protocol.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Awaitable, Callable

if TYPE_CHECKING:
    from broker.validator import EventValidator

from models import WarehouseEvent

# Handler signature: an async callable accepting a topic string and a WarehouseEvent
EventHandler = Callable[[str, WarehouseEvent], Awaitable[None]]


class EventBus(ABC):
    """Abstract Event Bus interface for decoupling agent message exchanges."""

    @abstractmethod
    def add_validator(self, validator: EventValidator) -> None:
        """
        Registers an inter-stage event validator gate.
        Events failing validation are blocked from reaching downstream agents.
        """
        pass

    @abstractmethod
    async def publish(self, topic: str, event: WarehouseEvent) -> None:
        """
        Publishes a WarehouseEvent to a specific topic key.

        Args:
            topic: Dot-delimited routing key (e.g. 'warehouse.inbound.pallet_received').
            event: The WarehouseEvent envelope.
        """
        pass

    @abstractmethod
    async def subscribe(self, topic_pattern: str, handler: EventHandler) -> None:
        """
        Subscribes an asynchronous handler to a topic or wildcard pattern.

        Supports AMQP-style wildcards:
          - '*' matches exactly one word (e.g. 'warehouse.inbound.*')
          - '#' matches zero or more words (e.g. 'warehouse.#')

        Args:
            topic_pattern: Routing pattern to match against published topics.
            handler: Async callback function receiving (topic, event).
        """
        pass

    @abstractmethod
    async def start(self) -> None:
        """Starts background dispatch queues and worker loops."""
        pass

    @abstractmethod
    async def stop(self) -> None:
        """Gracefully shuts down workers and drains queues."""
        pass
