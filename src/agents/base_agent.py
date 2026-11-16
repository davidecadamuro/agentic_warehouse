"""
Warehouse Swarm Simulation - Base Agent with OpenAI SDK.

Provides BaseAgent: an autonomous agent that interfaces with OpenAI's Chat Completions
API using function calling / tool use. It connects to the EventBus, listens for domain
events, drives the reasoning/tool execution loop, and publishes resulting events.
"""

from __future__ import annotations

import json
import logging
import os
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, List, Optional

from openai import AsyncOpenAI
from openai.types.chat import ChatCompletionMessageToolCall

from broker.base import EventBus, EventHandler
from models import WarehouseEvent

logger = logging.getLogger("warehouse.agent")


class BaseAgent(ABC):
    """
    Abstract base agent integrating the OpenAI Chat Completions SDK with the EventBus.
    
    Subclasses define their system prompt, registered tool schemas, tool execution
    mappings, and event bus subscriptions.
    """

    def __init__(
        self,
        name: str,
        system_prompt: str,
        event_bus: EventBus,
        model: str = "gpt-4o-mini",
        client: Optional[AsyncOpenAI] = None,
        mock_mode: Optional[bool] = None,
    ) -> None:
        self.name = name
        self.system_prompt = system_prompt
        self.event_bus = event_bus
        self.model = model

        # Determine whether to use real OpenAI client or offline simulation fallback
        api_key = os.getenv("OPENAI_API_KEY")
        if mock_mode is not None:
            self.mock_mode = mock_mode
        else:
            self.mock_mode = not bool(api_key or client)

        if client:
            self.client = client
        elif not self.mock_mode:
            self.client = AsyncOpenAI(api_key=api_key)
        else:
            self.client = None  # Mock mode will handle reasoning deterministically

        self._tools_schema: List[Dict[str, Any]] = []
        self._tool_handlers: Dict[str, Callable[..., Any]] = {}

    async def publish(self, topic: str, event: WarehouseEvent) -> None:
        """Publishes an event to the underlying event bus with agent attribution."""
        if not event.source_agent or event.source_agent == "SYSTEM":
            event.source_agent = self.name
        await self.event_bus.publish(topic, event)
        logger.debug(f"[{self.name}] Emitted event '{event.event_type.value}' on topic '{topic}'")

    async def subscribe(self, topic_pattern: str, handler: EventHandler) -> None:
        """Subscribes an event handler to a topic or wildcard pattern on the event bus."""
        await self.event_bus.subscribe(topic_pattern, handler)
        logger.info(f"[{self.name}] Subscribed handler '{handler.__qualname__}' to '{topic_pattern}'")

    def register_tool(
        self,
        name: str,
        description: str,
        parameters: Dict[str, Any],
        handler: Callable[..., Any],
    ) -> None:
        """Registers a deterministic Python tool and its corresponding OpenAI function schema."""
        self._tools_schema.append({
            "type": "function",
            "function": {
                "name": name,
                "description": description,
                "parameters": parameters,
            },
        })
        self._tool_handlers[name] = handler

    @abstractmethod
    async def initialize(self) -> None:
        """Register topic subscriptions on the event bus."""
        pass

    async def execute_turn(
        self, user_prompt: str, correlation_id: Optional[str] = None
    ) -> str:
        """
        Executes an agent reasoning turn.
        If live OpenAI client is available, runs standard chat.completions function calling loop.
        If mock_mode is True, delegates to self.mock_reasoning() for offline simulation.
        """
        logger.info(f"[{self.name}] Turn started. Prompt: {user_prompt[:80]}...")

        if self.mock_mode or not self.client:
            return await self.mock_reasoning(user_prompt, correlation_id)

        messages: List[Dict[str, Any]] = [
            {"role": "system", "content": self.system_prompt},
            {"role": "user", "content": user_prompt},
        ]

        # Multi-step tool invocation loop
        max_iterations = 10
        iteration = 0

        while iteration < max_iterations:
            iteration += 1
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=messages,  # type: ignore[arg-type]
                tools=self._tools_schema if self._tools_schema else None,  # type: ignore[arg-type]
            )

            response_message = response.choices[0].message
            tool_calls: Optional[List[ChatCompletionMessageToolCall]] = response_message.tool_calls

            # Append assistant message
            messages.append(response_message.to_dict())  # type: ignore[arg-type]

            # If no further tool calls requested, we are done
            if not tool_calls:
                final_content = response_message.content or ""
                logger.info(f"[{self.name}] Final response: {final_content[:100]}...")
                return final_content

            # Execute tool calls requested by the model
            for tool_call in tool_calls:
                func_name = tool_call.function.name
                arguments_str = tool_call.function.arguments
                call_id = tool_call.id

                try:
                    args = json.loads(arguments_str) if arguments_str else {}
                except json.JSONDecodeError:
                    args = {}

                logger.info(f"[{self.name}] Calling tool '{func_name}' with args {args}")
                handler = self._tool_handlers.get(func_name)
                if handler:
                    try:
                        tool_result = handler(**args)
                    except Exception as err:
                        tool_result = {"error": f"Tool execution failed: {err}"}
                else:
                    tool_result = {"error": f"Unknown tool '{func_name}'"}

                # Provide tool output back to model
                messages.append({
                    "role": "tool",
                    "tool_call_id": call_id,
                    "content": json.dumps(tool_result),
                })

        return "Max tool call iterations reached."

    @abstractmethod
    async def mock_reasoning(
        self, prompt: str, correlation_id: Optional[str] = None
    ) -> str:
        """
        Deterministic fallback reasoning implementation when no API key is supplied.
        Ensures simulation scenarios remain 100% executable and testable offline.
        """
        pass
