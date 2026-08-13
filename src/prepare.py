"""Acquire keeper inventory, then prepare pool allowances."""

import argparse
import asyncio
import json
import logging
import os
from pathlib import Path

import httpx
import yaml
from dotenv import load_dotenv
from web3 import Web3

from helpers import Pool, TransactionError, TransactionLane

logger = logging.getLogger(__name__)
MAX_UINT = 2**256 - 1
ONEINCH_NATIVE_TOKEN = "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE"
ONEINCH_ROUTER = Web3.to_checksum_address("0x111111125421cA6dc452d289314280a0f8842A65")


class _OneInchThrottle:
    def __init__(self, interval_seconds):
        self.interval_seconds = interval_seconds
        self.last_request_at = None

    async def get(self, client, url, **kwargs):
        loop = asyncio.get_running_loop()
        if self.last_request_at is not None:
            delay = self.last_request_at + self.interval_seconds - loop.time()
            if delay > 0:
                await asyncio.sleep(delay)
        self.last_request_at = loop.time()
        return await client.get(url, **kwargs)


def _approval_state(chain, pool_config, coin_index, token, state):
    result = {
        "phase": "approval",
        "chain": chain["name"],
        "pool": pool_config["id"],
        "coin_index": coin_index,
        "state": state,
    }
    if token is not None:
        result["token"] = token
    return result


def _bounded_batch(intents, maximum):
    if len(intents) > maximum:
        raise TransactionError(
            f"preparation batch has {len(intents)} intents; configured limit is {maximum}"
        )
    return intents


async def _request_oneinch_swap(
    client,
    throttle,
    *,
    api_key,
    chain_id,
    owner_address,
    destination_token,
    amount,
    slippage_bps,
):
    response = await throttle.get(
        client,
        f"https://api.1inch.dev/swap/v6.0/{chain_id}/swap",
        params={
            "src": ONEINCH_NATIVE_TOKEN,
            "dst": destination_token,
            "amount": amount,
            "from": owner_address,
            "receiver": owner_address,
            "slippage": slippage_bps / 100,
            "allowPartialFill": "false",
            "includeTokensInfo": "true",
            "disableEstimate": "false",
        },
        headers={"Authorization": f"Bearer {api_key}"},
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise TransactionError("invalid 1inch response")
    return payload


def _oneinch_uint(value, field):
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise TransactionError(f"invalid 1inch {field}")
    try:
        parsed = int(value)
    except ValueError:
        raise TransactionError(f"invalid 1inch {field}") from None
    if parsed < 0:
        raise TransactionError(f"invalid 1inch {field}")
    return parsed


def _validate_oneinch_swap(
    response,
    *,
    owner_address,
    destination_token,
    input_amount,
):
    try:
        source = Web3.to_checksum_address(response["srcToken"]["address"])
        destination = Web3.to_checksum_address(response["dstToken"]["address"])
        output = _oneinch_uint(response["dstAmount"], "dstAmount")
        transaction = response["tx"]
        sender = Web3.to_checksum_address(transaction["from"])
        router = Web3.to_checksum_address(transaction["to"])
        value = _oneinch_uint(transaction["value"], "transaction value")
        data = transaction["data"]
    except (KeyError, TypeError, ValueError):
        raise TransactionError("invalid 1inch swap envelope") from None
    if (
        source != Web3.to_checksum_address(ONEINCH_NATIVE_TOKEN)
        or destination != Web3.to_checksum_address(destination_token)
        or output < 1
        or sender != Web3.to_checksum_address(owner_address)
        or router != ONEINCH_ROUTER
        or value != input_amount
        or not isinstance(data, str)
        or not data.startswith("0x")
        or len(data) < 10
    ):
        raise TransactionError("unexpected 1inch swap envelope")
    try:
        bytes.fromhex(data[2:])
    except ValueError:
        raise TransactionError("invalid 1inch transaction data") from None
    return transaction, output


async def _collect_acquisitions(
    targets,
    token_contracts,
    app,
    chain,
    lane,
    api_key,
    client,
    throttle,
    dry_run,
):
    queued = []
    results = []
    w3 = lane.w3
    owner_address = lane.address
    available_native = await w3.eth.get_balance(owner_address)
    logger.info("%s checking %d configured input tokens", chain["name"], len(targets))
    for token_address, target_amount in targets.items():
        try:
            token_contract = token_contracts[token_address]
            token_balance = await token_contract.functions.balanceOf(owner_address).call()
            if token_balance >= target_amount:
                logger.info("%s input token %s: already funded", chain["name"], token_address)
                results.append(
                    {
                        "phase": "acquisition",
                        "chain": chain["name"],
                        "token": token_address,
                        "state": "funded",
                    }
                )
                continue

            maximum_input = min(
                available_native * app["acquisition_max_balance_bps"] // 10_000,
                available_native - app["gas_reserve_wei"],
            )
            if maximum_input <= 0:
                raise TransactionError("gas reserve reached")

            logger.info("%s acquiring %s: sizing 1inch quote", chain["name"], token_address)
            swap = await _request_oneinch_swap(
                client,
                throttle,
                api_key=api_key,
                chain_id=chain["chain_id"],
                owner_address=owner_address,
                destination_token=token_address,
                amount=maximum_input,
                slippage_bps=app["slippage_bps"],
            )
            transaction, quoted_output = _validate_oneinch_swap(
                swap,
                owner_address=owner_address,
                destination_token=token_address,
                input_amount=maximum_input,
            )
            input_amount = min(
                maximum_input,
                max(
                    1,
                    maximum_input
                    * (target_amount - token_balance)
                    * app["acquisition_buffer_bps"]
                    // quoted_output
                    // 10_000,
                ),
            )
            if input_amount < maximum_input:
                logger.info(
                    "%s acquiring %s: quoting exact input=%d wei",
                    chain["name"],
                    token_address,
                    input_amount,
                )
                swap = await _request_oneinch_swap(
                    client,
                    throttle,
                    api_key=api_key,
                    chain_id=chain["chain_id"],
                    owner_address=owner_address,
                    destination_token=token_address,
                    amount=input_amount,
                    slippage_bps=app["slippage_bps"],
                )
                transaction, _ = _validate_oneinch_swap(
                    swap,
                    owner_address=owner_address,
                    destination_token=token_address,
                    input_amount=input_amount,
                )

            # 1inch owns route calldata semantics; exposed fields stay independently checked.
            intent = {
                "label": f"acquire:{token_address}",
                "to": ONEINCH_ROUTER,
                "data": transaction["data"],
                "value": input_amount,
            }
            result = {
                "phase": "acquisition",
                "chain": chain["name"],
                "token": token_address,
                "eth_in": input_amount,
            }
            if dry_run:
                await w3.eth.call(
                    {
                        "from": owner_address,
                        "to": intent["to"],
                        "data": intent["data"],
                        "value": input_amount,
                    }
                )
                result["state"] = "dry_run"
                logger.info("%s acquiring %s: simulation passed", chain["name"], token_address)
                results.append(result)
            else:
                queued.append((intent, result))
                available_native -= input_amount
        except Exception as exc:  # noqa: BLE001 - isolate one token acquisition
            logger.error("%s acquisition failed: %s", token_address, exc)
            results.append(
                {
                    "phase": "acquisition",
                    "chain": chain["name"],
                    "token": token_address,
                    "state": "failed",
                }
            )
    return queued, results


async def _inspect_pools(app, chain, lane):
    targets = {}
    target_tokens = {}
    approvals = []
    results = []
    seen_approvals = set()
    for pool_config in app["pools"]:
        if pool_config["chain_id"] != chain["chain_id"]:
            continue
        try:
            pool = Pool(pool_config, lane.w3, app["slippage_bps"])
            await pool.initialize()
            target = pool.coin_addresses[pool.target_coin_index]
            targets[target] = targets.get(target, 0) + pool.input_amounts[pool.target_coin_index]
            target_tokens.setdefault(target, pool.token_contracts[pool.target_coin_index])
        except Exception as exc:  # noqa: BLE001 - isolate one configured pool
            result = _approval_state(chain, pool_config, None, None, "inspection_failed")
            result["error"] = type(exc).__name__
            results.append(result)
            continue

        for coin_index, (token_address, token_contract, required_allowance) in enumerate(
            zip(pool.coin_addresses, pool.token_contracts, pool.input_amounts, strict=True)
        ):
            try:
                allowance = await token_contract.functions.allowance(
                    lane.address, pool.address
                ).call()
                if allowance >= required_allowance:
                    logger.info(
                        "%s %s coin%d: allowance sufficient",
                        chain["name"],
                        pool_config["id"],
                        coin_index,
                    )
                    results.append(
                        _approval_state(
                            chain,
                            pool_config,
                            coin_index,
                            token_address,
                            "allowance_sufficient",
                        )
                    )
                    continue

                approval_key = (token_address, pool.address)
                if approval_key in seen_approvals:
                    results.append(
                        _approval_state(
                            chain,
                            pool_config,
                            coin_index,
                            token_address,
                            "approval_duplicate",
                        )
                    )
                    continue
                seen_approvals.add(approval_key)

                result = _approval_state(
                    chain, pool_config, coin_index, token_address, "approval_pending"
                )
                logger.info(
                    "%s %s coin%d: approval required",
                    chain["name"],
                    pool_config["id"],
                    coin_index,
                )
                approvals.append(
                    (
                        {
                            "label": f"approve:{chain['chain_id']}:{token_address}:{pool.address}",
                            "to": token_address,
                            "data": token_contract.encode_abi(
                                "approve", args=[pool.address, MAX_UINT]
                            ),
                        },
                        result,
                    )
                )
            except Exception as exc:  # noqa: BLE001 - isolate one pool coin
                result = _approval_state(
                    chain, pool_config, coin_index, token_address, "inspection_failed"
                )
                result["error"] = type(exc).__name__
                results.append(result)

    return targets, target_tokens, approvals, results


async def _prepare_chain(
    app,
    chain,
    api_key,
    client,
    throttle,
    dry_run,
):
    lane = None
    try:
        lane = TransactionLane(app, chain, os.environ)
        await lane.initialize()
        targets, target_tokens, approval_queued, results = await _inspect_pools(app, chain, lane)
        acquisition_queued, acquisition_results = await _collect_acquisitions(
            targets,
            target_tokens,
            app,
            chain,
            lane,
            api_key,
            client,
            throttle,
            dry_run,
        )
        queued = acquisition_queued + approval_queued
        results = acquisition_results + results

        if queued and not dry_run:
            batch = _bounded_batch(queued, chain["max_batch_size"])
            try:
                submitted = await lane.submit([intent for intent, _ in batch])
            except Exception as exc:  # noqa: BLE001 - a failed batch marks every intent rejected
                logger.error("%s batch submit failed: %s", chain["name"], exc)
                submitted = []
            returned = {record["label"]: record for record in submitted}
            for intent, result in batch:
                record = returned.get(intent["label"])
                if record is not None:
                    result["state"] = (
                        "submitted" if result["phase"] == "acquisition" else "approval_submitted"
                    )
                    result["tx_hash"] = record["tx_hash"]
                else:
                    result["state"] = (
                        "failed" if result["phase"] == "acquisition" else "approval_rejected"
                    )
        results.extend(result for _, result in queued)
        return results
    except Exception as exc:  # noqa: BLE001 - independent chains must continue
        return [
            {
                "phase": "approval",
                "chain": chain["name"],
                "state": "chain_failed",
                "error": type(exc).__name__,
            }
        ]
    finally:
        if lane is not None:
            try:
                await lane.w3.provider.disconnect()
            except Exception as exc:  # noqa: BLE001 - cleanup must not hide prior results
                logger.error("%s RPC disconnect failed (%s)", chain["name"], type(exc).__name__)


async def run_preparation(path="config.yaml", dry_run=False):
    load_dotenv()
    app = yaml.safe_load(Path(path).read_text())
    api_key = os.environ.get("ONEINCH_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("ONEINCH_API_KEY is required")
    throttle = _OneInchThrottle(app["acquisition_api_interval_seconds"])
    results = []

    async with httpx.AsyncClient(timeout=app["http_timeout_seconds"]) as client:
        for chain in app["chains"]:
            results.extend(
                await _prepare_chain(
                    app,
                    chain,
                    api_key=api_key,
                    client=client,
                    throttle=throttle,
                    dry_run=dry_run,
                )
            )
    return results


def main():
    parser = argparse.ArgumentParser(
        description="Acquire keeper inventory, then approve configured pools."
    )
    parser.add_argument("--config", default="config.yaml", help="configuration file")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="quote acquisitions and inspect allowances without sending transactions",
    )
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    for result in asyncio.run(run_preparation(args.config, args.dry_run)):
        print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
