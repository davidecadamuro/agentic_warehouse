# Warehouse Agentic Swarm Architecture

A Python-based mock warehouse orchestration system built around an event-driven swarm of agent workflows. The project models inbound receiving, storage putaway, transport dispatch, outbound order fulfillment, exception handling, and ledger-based inventory auditing using a hybrid architecture: autonomous agents reason over unstructured operational context while deterministic tools enforce business rules and physical constraints.

## Why this project exists

I've creaed this repository to prepare to a job interview.

Traditional warehouse automation is usually built around rigid deterministic pipelines. This project explores a more adaptive model where autonomous agents handle operational ambiguity while core invariants remain enforced by code.

The architecture separates:

- Agent reasoning: interpreting disruptions, planning responses, and invoking tools
- Deterministic tool layer: validating constraints, maintaining the ledger, and updating physical state
- Event bus coordination: passing events among agents without tightly coupling them

## Project structure

```text
.
├── docs/
│   ├── agents.md
│   ├── architecture.md
│   ├── auditability.md
│   ├── broker.md
│   ├── database.md
│   └── tools.md
├── src/
│   ├── agents/
│   ├── broker/
│   ├── database.py
│   ├── models.py
│   ├── simulation.py
│   ├── tests/
│   └── tools.py
├── .gitignore
├── README.md
├── requirements.txt
├── resource.txt
└── .venv/
```

## Core concepts

### Agent layer
Each agent inherits from `BaseAgent` and integrates with the OpenAI function-calling flow. In mock mode, the system uses deterministic offline reasoning so the project can run without an API key.

### Event-driven broker
The broker routes domain events such as shipment arrivals, putaway requests, transport completions, and order dispatches across the warehouse swarm.

### Deterministic tools
The tool layer enforces warehouse rules, including:

- valid transport routing
- stock reservation and allocation
- bin and corridor state changes
- inventory ledger consistency
- pallet quarantine and exception handling

### Auditability
The system maintains a ledger and correlation identifiers so operational sequences can be traced and inspected after the fact.

## Agents in the system

- ReceivingAgent: processes inbound shipments and quarantines damaged pallets
- PutawayAgent: finds bins and reserves putaway space
- TransportAgent: dispatches vehicles and completes missions
- InventoryAgent: verifies stock and inventory summaries
- OrderFulfillmentAgent: reserves stock and dispatches pick operations
- ExceptionAgent: handles blocked corridors and exception conditions

## Quick start

1. Create and activate a virtual environment if needed.
2. Install dependencies:

```bash
python -m pip install -r requirements.txt
```

3. Run the simulation:

```bash
python src/simulation.py
```

4. To run the test suite:

```bash
pytest -q
```

## Live LLM mode and runtime configuration

The simulation supports explicit runtime configuration for both the mock/offline mode and the OpenAI model.

### Using the CLI

```bash
python src/simulation.py --mock-mode true --model gpt-4o-mini
```

```bash
python src/simulation.py --mock-mode false --model gpt-4.1-mini
```

### Using environment variables

```bash
export OPENAI_API_KEY="your-key-here"
export OPENAI_MODEL="gpt-4o-mini"
export WAREHOUSE_MOCK_MODE="False"
python src/simulation.py
```

If `WAREHOUSE_MOCK_MODE` is not set, the project falls back to the presence of `OPENAI_API_KEY`:
- `OPENAI_API_KEY` missing => mock mode is enabled automatically
- `OPENAI_API_KEY` present => live OpenAI mode is used unless overridden by the CLI or environment

If `OPENAI_MODEL` is not set, the default model is `gpt-6-luna`.

## Documentation

See the docs folder for deeper design explanations:

- [docs/architecture.md](docs/architecture.md)
- [docs/agents.md](docs/agents.md)
- [docs/broker.md](docs/broker.md)
- [docs/database.md](docs/database.md)
- [docs/tools.md](docs/tools.md)
- [docs/auditability.md](docs/auditability.md)


