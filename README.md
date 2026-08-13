# FX Upkeep

Backend for FX pools: EOA token acquisition and approvals, plus a minimal multi-chain,
multi-pool keeper runner that advances lazy EMAs toward current dynamic state so pool quotes
stay fresh.

## Setup

Requires Python 3.12+ and [uv](https://docs.astral.sh/uv/).

```bash
cp .env.example .env
uv sync
```

Set the required API credentials and keeper private key in `.env`. Configure chains, pools, and
upkeep frequency in `config.yaml`.

## Run

Prepare keeper inventory and pool allowances:

```bash
uv run src/prepare.py
```

Preparation sums configured input requirements by token and acquires only those input tokens. It
builds one sequential batch per chain from a single pending nonce — acquisitions first, then
approvals — producing consecutive transactions without checking receipts. It approves both pool
coins so reverse-side inventory can be swapped back after the first pool swap.

Run the keeper:

```bash
uv run src/keeper.py
```

Read-only checks:

```bash
uv run src/prepare.py --dry-run
uv run src/keeper.py --validate-only
```

Only one process may use the keeper EOA at a time because concurrent nonce allocation can collide.
Each heartbeat reads the latest mined nonce and quotes fees fresh, then submits due pool swaps in
one batch; if a transaction is still pending at that nonce, the new transaction attempts to
replace it. A batch reserves observed token balances so two pools cannot independently spend the
same inventory. Swap direction is chosen from live balances: the configured side when fully
funded, otherwise the reverse side, before a bounded partial swap.

Transactions are stateless — no hashes or receipts are retained. For EIP-1559 blocks,
`maxFeePerGas` starts at twice the base fee (raised only as needed to cover base plus priority),
with priority the greater of 5% of the base fee, the node's suggested tip, or one wei; at a zero
base fee both fee fields use the greater of node gas price, node tip, or one wei. Ankr is the
primary RPC; dRPC is an optional transport fallback.
