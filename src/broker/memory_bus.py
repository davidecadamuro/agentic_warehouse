"""
Warehouse Swarm Simulation - In-Memory Asynchronous Event Broker.

Implements the EventBus interface using Python's asyncio primitives.
Features topic-based publish/subscribe routing with AMQP-style wildcards
('*' for one word, '#' for zero or more words), asynchronous queue dispatch,
and a Dead-Letter Queue (DLQ) for failed consumer handlers.
"""

from __future__ import annotations

import asyncio
import logging
import re
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Deque, List, Optional, Tuple

from broker.base import EventBus, EventHandler
from broker.validator import EventValidator, WarehouseDomainValidator
from models import WarehouseEvent

logger = logging.getLogger("warehouse.broker.memory")


@dataclass
class DeadLetterEntry:
    """Record of an event handler failure routed to the Dead-Letter Queue."""
    topic: str
    event: WarehouseEvent
    handler_name: str
    error: str
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


def _compile_topic_pattern(pattern: str) -> re.Pattern[str]:
    """
    Translates an AMQP-style topic pattern into a regular expression.
    - '.' is escaped as literal dot
    - '*' matches exactly one word token [^.]+
    - '#' matches zero or more word tokens (.*)
    """
    tokens = pattern.split(".")
    regex_parts = []
    for token in tokens:
        if token == "*":
            regex_parts.append(r"[^.]+")
        elif token == "#":
            regex_parts.append(r".*")
        else:
            regex_parts.append(re.escape(token))
    regex = r"^" + r"\.".join(regex_parts) + r"$"
    # Normalize cases where # matches empty segments
    regex = regex.replace(r"\..*", r"(\..*)?")
    return re.compile(regex)


class InMemoryEventBus(EventBus):
    """
    Thread-safe asynchronous in-memory event bus with topic wildcard matching,
    inter-stage validation guardrails, and dead-letter queue exception containment.
    """

    def __init__(
        self,
        queue_maxsize: int = 1000,
        enable_default_validation: bool = True,
        max_history_size: int = 1000,
        max_dlq_size: int = 500,
        max_rejected_size: int = 500,
    ) -> None:
        self._subscriptions: List[Tuple[str, re.Pattern[str], EventHandler]] = []
        self._queue: asyncio.Queue[Tuple[str, WarehouseEvent]] = asyncio.Queue(maxsize=queue_maxsize)
        self._worker_task: Optional[asyncio.Task[None]] = None
        self._running: bool = False
        self.dlq: Deque[DeadLetterEntry] = deque(maxlen=max_dlq_size)
        self.history: Deque[Tuple[str, WarehouseEvent]] = deque(maxlen=max_history_size)
        self._validators: List[EventValidator] = []
        self.rejected_events: Deque[Tuple[str, WarehouseEvent, str]] = deque(maxlen=max_rejected_size)

        if enable_default_validation:
            self.add_validator(WarehouseDomainValidator())

    def add_validator(self, validator: EventValidator) -> None:
        """Registers an inter-stage event validator gate."""
        self._validators.append(validator)
        logger.info(f"Registered event validator gate '{validator.name}'.")

    async def start(self) -> None:
        """Starts the background event dispatch loop."""
        if self._running:
            return
        self._running = True
        self._worker_task = asyncio.create_task(self._dispatch_loop())
        logger.info("InMemoryEventBus started.")

    async def stop(self) -> None:
        """Stops the event bus and drains any remaining events."""
        if not self._running:
            return
        self._running = False
        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
            self._worker_task = None
        logger.info("InMemoryEventBus stopped.")

    async def subscribe(self, topic_pattern: str, handler: EventHandler) -> None:
        """Registers a handler for a topic pattern."""
        regex = _compile_topic_pattern(topic_pattern)
        self._subscriptions.append((topic_pattern, regex, handler))
        logger.debug(f"Subscribed handler '{handler.__qualname__}' to pattern '{topic_pattern}'")

    async def publish(self, topic: str, event: WarehouseEvent) -> None:
        """
        Publishes an event to the queue for asynchronous fan-out dispatch,
        passing through all registered inter-stage validator gates first.
        """
        # Run inter-stage validation gates
        if topic != "warehouse.validation.rejected":
            for validator in self._validators:
                result = validator.validate(topic, event)
                if not result.is_valid:
                    error_msg = result.error or "Unknown validation error"
                    logger.warning(
                        f"[ValidationGate] Event '{event.event_type.value}' on topic '{topic}' "
                        f"REJECTED by '{validator.name}': {error_msg}"
                    )
                    self.rejected_events.append((topic, event, error_msg))

                    # Route rejection to dead-letter rejection event for auditing
                    rejection_event = WarehouseEvent(
                        event_type=event.event_type,
                        payload={
                            "rejected_topic": topic,
                            "original_payload": event.payload,
                            "rejection_reason": error_msg,
                            "validator": validator.name,
                        },
                        correlation_id=event.correlation_id,
                        source_agent="VALIDATION_GATE",
                    )
                    await self._queue.put(("warehouse.validation.rejected", rejection_event))
                    return

        self.history.append((topic, event))
        await self._queue.put((topic, event))
        logger.debug(f"Published event '{event.event_type.value}' to topic '{topic}' [correlation: {event.correlation_id}]")

    async def _dispatch_loop(self) -> None:
        """Background worker consuming from queue and routing to matching subscriber handlers."""
        while self._running:
            try:
                topic, event = await self._queue.get()
                matching_handlers = [
                    handler
                    for _, regex, handler in self._subscriptions
                    if regex.match(topic)
                ]

                # Dispatch concurrently to all matching subscribers
                if matching_handlers:
                    tasks = [
                        self._safe_invoke_handler(handler, topic, event)
                        for handler in matching_handlers
                    ]
                    await asyncio.gather(*tasks)

                self._queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Unexpected error in event bus dispatch loop: {e}", exc_info=True)

    async def _safe_invoke_handler(
        self, handler: EventHandler, topic: str, event: WarehouseEvent
    ) -> None:
        """Invokes a handler and routes unhandled exceptions to the Dead-Letter Queue."""
        handler_name = getattr(handler, "__qualname__", str(handler))
        try:
            await handler(topic, event)
        except Exception as err:
            logger.error(
                f"Handler '{handler_name}' failed on topic '{topic}': {err}",
                exc_info=True,
            )
            self.dlq.append(
                DeadLetterEntry(
                    topic=topic,
                    event=event,
                    handler_name=handler_name,
                    error=str(err),
                )
            )

    async def wait_until_idle(self) -> None:
        """Convenience method for tests and simulations to wait until the queue is fully drained."""
        await self._queue.join()
