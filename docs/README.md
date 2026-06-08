# Crypto Codebase ETL — Documentation

Beginner-friendly deep dives into this repository. Read in any order, or follow the suggested path below.

## Topics

| # | Document | What you'll learn |
|---|----------|-------------------|
| 1 | [Business Purpose](./business-purpose.md) | Why this project exists and what problems it solves |
| 2 | [Architecture](./architecture.md) | High-level system design and how pieces connect |
| 3 | [Repo Structure](./repo-structure.md) | What every folder and file group does |
| 4 | [End-to-End Workflow](./end-to-end-workflow.md) | Step-by-step journey of data through the pipeline |
| 5 | [Tech Stack](./tech-stack.md) | Every technology used and why it was chosen |
| 6 | [Data Flow](./data-flow.md) | How data moves, transforms, and lands in storage |
| 7 | [Key Components](./key-components.md) | Deep dive into each service and module |
| 8 | [Deployment](./deployment.md) | How to run locally with Docker and Makefile |
| 9 | [Concepts](./concepts.md) | Important data-engineering ideas used in this project |

## Suggested reading order for beginners

1. **Business Purpose** — understand the "why"
2. **Architecture** — see the big picture
3. **Concepts** — learn terms like DLQ, watermark, OHLC
4. **End-to-End Workflow** — follow one trade from Binance to the dashboard
5. **Repo Structure** + **Key Components** — explore the code
6. **Tech Stack** + **Data Flow** + **Deployment** — run and operate it

## Related files in the repo

- [README.md](../README.md) — quick start and troubleshooting
- [.env.example](../.env.example) — configuration reference
- [schemas/trade_event.json](../schemas/trade_event.json) — data contract
