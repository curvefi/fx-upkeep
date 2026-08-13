import argparse
import asyncio
import logging
import os
import signal
from dataclasses import dataclass
from pathlib import Path

import yaml
from dotenv import load_dotenv

from helpers import Pool, SkipPool, TransactionLane

logger = logging.getLogger(__name__)


def load_config(path):
    return yaml.safe_load(Path(path).read_text())


@dataclass
class PoolState:
    pool: Pool
    last_upkeep_timestamp: int = 0


class ChainWorker:
    def __init__(self, app, chain):
        self.app, self.chain = app, chain
        self.transaction_lane = TransactionLane(app, chain, os.environ)
        self.pool_states = []

    async def initialize(self):
        await self.transaction_lane.initialize()
        for pool_config in self.app["pools"]:
            if pool_config["chain_id"] != self.chain["chain_id"]:
                continue
            try:
                pool = Pool(pool_config, self.transaction_lane.w3, self.app["slippage_bps"])
                await pool.initialize()
                self.pool_states.append(PoolState(pool))
            except Exception:
                logger.exception("%s skipped %s", self.chain["name"], pool_config["id"])
        logger.info("%s validated %d pools", self.chain["name"], len(self.pool_states))

    async def _refresh_last_upkeep_timestamps(self, block_number):
        timestamps = await asyncio.gather(
            *(state.pool.fetch_last_upkeep_timestamp(block_number) for state in self.pool_states),
            return_exceptions=True,
        )
        refreshed_states = []
        for state, timestamp in zip(self.pool_states, timestamps, strict=True):
            if isinstance(timestamp, Exception):
                logger.error(
                    "%s failed to read %s timestamp: %s",
                    self.chain["name"],
                    state.pool.id,
                    timestamp,
                )
                continue
            state.last_upkeep_timestamp = timestamp
            refreshed_states.append(state)
        return refreshed_states

    def _select_candidate_states(self, refreshed_states, block_timestamp):
        candidate_states = [
            state
            for state in refreshed_states
            if block_timestamp >= state.last_upkeep_timestamp + state.pool.upkeep_interval_seconds
        ]
        candidate_states.sort(key=lambda state: (state.last_upkeep_timestamp, state.pool.id))
        return candidate_states

    async def _prepare_intents(self, candidate_states):
        prepared_intents = []
        balance_limits = {}
        for state in candidate_states:
            if len(prepared_intents) >= self.chain["max_batch_size"]:
                break
            pool = state.pool
            try:
                intent, input_token, remaining_balance = await pool.prepare_intent(
                    self.transaction_lane.address, balance_limits
                )
                prepared_intents.append(intent)
                balance_limits[input_token] = remaining_balance
            except SkipPool as exc:
                logger.info("%s %s: %s", self.chain["name"], pool.id, exc)
            except Exception:
                logger.exception("%s failed to prepare %s", self.chain["name"], pool.id)
        return prepared_intents

    async def heartbeat(self):
        latest_block = await self.transaction_lane.w3.eth.get_block("latest")
        refreshed_states = await self._refresh_last_upkeep_timestamps(latest_block["number"])
        candidate_states = self._select_candidate_states(
            refreshed_states, latest_block["timestamp"]
        )
        prepared_intents = await self._prepare_intents(candidate_states)
        submitted = await self.transaction_lane.submit(prepared_intents, nonce_block="latest")
        if not submitted:
            logger.info("%s idle", self.chain["name"])

    async def run_until_stopped(self, stop, once):
        if once:
            await self.heartbeat()
            return
        while not stop.is_set():
            try:
                await self.heartbeat()
            except Exception:
                logger.exception("%s keeper cycle failed", self.chain["name"])
            delay = self.app["heartbeat_seconds"]
            try:
                await asyncio.wait_for(stop.wait(), delay)
            except TimeoutError:
                pass


async def run_keeper(path, once=False, validate=False):
    load_dotenv()
    app = load_config(path)
    workers = [ChainWorker(app, chain) for chain in app["chains"]]
    try:
        await asyncio.gather(*(worker.initialize() for worker in workers))
        if validate:
            return

        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set)
        await asyncio.gather(*(worker.run_until_stopped(stop, once) for worker in workers))
    finally:
        await asyncio.gather(
            *(worker.transaction_lane.w3.provider.disconnect() for worker in workers),
            return_exceptions=True,
        )


def main():
    parser = argparse.ArgumentParser(description="Run Curve pool upkeep heartbeats.")
    parser.add_argument("--config", default="config.yaml", help="configuration file")
    parser.add_argument("--once", action="store_true", help="run one heartbeat and exit")
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="initialize configured chains and pools, then exit",
    )
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    asyncio.run(run_keeper(args.config, args.once, args.validate_only))


if __name__ == "__main__":
    main()
