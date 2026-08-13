# FX Upkeep

Backend for FX pools: EOA token acquisition and approvals, plus a minimal multi-chain,
multi-pool keeper runner.

The goal is to advance lazy EMAs into current dynamic state, keeping pool quotes fresh and
avoiding stale prices.

## Setup

Requires Python 3.12+ and [uv](https://docs.astral.sh/uv/).

```bash
cp .env.example .env
uv sync
```

Set the required API credentials and keeper private key in `.env`. Configure chains, pools, and
upkeep frequency in `config.yaml`.

## Run

Prepare token inventory and pool allowances manually:

```bash
uv run src/prepare.py
```

Preparation sums configured input requirements by token and acquires only those input tokens.
The first pool swap creates reverse-side inventory; both pool coins are approved so that inventory
can be swapped back.

Run the keeper:

```bash
uv run src/keeper.py
```

Read-only checks:

```bash
uv run src/prepare.py --dry-run
uv run src/keeper.py --validate-only
```

Only one process may use the keeper EOA at a time. While its nonce lane is idle, the keeper scans
pools on the configured heartbeat. A batch reserves its observed token balances across pools, so
two pools cannot independently spend the same inventory. After submission the keeper waits up to
one minute for inclusion, then replaces each unresolved call at the same nonce with higher fees,
subject to the configured cap. Once the batch is included, it returns to the heartbeat.
Transaction hashes and batches exist only in memory. A restart forgets them, reads the latest
mined nonce, and does not inspect the pending nonce. Swap direction is selected from live balances:
the configured side is preferred when fully funded, otherwise the reverse side is tried before a
bounded partial swap. Ankr is the primary RPC; dRPC is an optional transport fallback.
