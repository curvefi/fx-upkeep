import argparse
import asyncio
import logging
import os
import signal
from pathlib import Path

import yaml
from dotenv import load_dotenv

from helpers import Pool, SkipPool, TransactionLane

logger = logging.getLogger(__name__)


class ChainWorker:
    def __init__(self, app, chain):
        self.app, self.chain = app, chain
        self.lane = TransactionLane(app, chain, os.environ)
        self.pools = []

    async def initialize(self):
        await self.lane.initialize()
        for pool_config in self.app["pools"]:
            if pool_config["chain_id"] != self.chain["chain_id"]:
                continue
            try:
                pool = Pool(pool_config, self.lane.w3, self.app["slippage_bps"])
                await pool.initialize()
                self.pools.append(pool)
            except Exception:
                logger.exception("%s skipped %s", self.chain["name"], pool_config["id"])
        logger.info("%s validated %d pools", self.chain["name"], len(self.pools))

    async def _prepare_intents(self, pools):
        intents = []
        balances = {}
        for pool in pools:
            if len(intents) >= self.chain["max_batch_size"]:
                break
            try:
                intent, token, balance = await pool.prepare_intent(self.lane.address, balances)
                intents.append(intent)
                balances[token] = balance
            except SkipPool as exc:
                logger.info("%s %s: %s", self.chain["name"], pool.id, exc)
            except Exception:
                logger.exception("%s failed to prepare %s", self.chain["name"], pool.id)
        return intents

    async def heartbeat(self):
        block = await self.lane.w3.eth.get_block("latest")
        timestamps = await asyncio.gather(
            *(pool.fetch_last_upkeep_timestamp(block["number"]) for pool in self.pools),
            return_exceptions=True,
        )
        due = []
        for pool, timestamp in zip(self.pools, timestamps, strict=True):
            if isinstance(timestamp, Exception):
                logger.error(
                    "%s failed to read %s timestamp: %s", self.chain["name"], pool.id, timestamp
                )
            else:
                gap = block["timestamp"] - timestamp
                logger.info(
                    "%s %s: last_ts=%d gap=%ds", self.chain["name"], pool.id, timestamp, gap
                )
                if gap >= pool.upkeep_interval_seconds:
                    due.append((timestamp, pool.id, pool))
        due.sort(key=lambda item: item[:2])
        intents = await self._prepare_intents(pool for _, _, pool in due)
        submitted = await self.lane.submit(intents, nonce_block="latest")
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
            try:
                await asyncio.wait_for(stop.wait(), self.app["heartbeat_seconds"])
            except TimeoutError:
                pass


async def run_keeper(path, once=False, validate=False):
    load_dotenv()
    app = yaml.safe_load(Path(path).read_text())
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
            *(worker.lane.w3.provider.disconnect() for worker in workers),
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
