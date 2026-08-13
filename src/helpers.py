"""Shared ABI, RPC, transaction-lane, and pool runtime."""

import asyncio
import logging
from decimal import Decimal

from eth_account import Account
from web3 import AsyncHTTPProvider, AsyncWeb3, Web3
from web3.exceptions import TimeExhausted, TransactionNotFound
from web3.providers import AsyncBaseProvider

__all__ = (
    "ERC20_ABI",
    "TWOCRYPTO_POOL_ABI",
    "NoIntentSurvived",
    "Pool",
    "SkipPool",
    "TransactionError",
    "TransactionLane",
)

logger = logging.getLogger(__name__)


def _function_abi(name, inputs, output, *, mutability="view"):
    return {
        "type": "function",
        "name": name,
        "stateMutability": mutability,
        "inputs": [{"name": str(index), "type": kind} for index, kind in enumerate(inputs)],
        "outputs": [{"name": "", "type": output}],
    }


TWOCRYPTO_POOL_ABI = [
    _function_abi("coins", ["uint256"], "address"),
    _function_abi("last_timestamp", [], "uint256"),
    _function_abi("get_dy", ["uint256", "uint256", "uint256"], "uint256"),
    _function_abi(
        "exchange",
        ["uint256", "uint256", "uint256", "uint256", "address"],
        "uint256",
        mutability="nonpayable",
    ),
]
ERC20_ABI = [
    _function_abi("balanceOf", ["address"], "uint256"),
    _function_abi("allowance", ["address", "address"], "uint256"),
    _function_abi("decimals", [], "uint8"),
    _function_abi("approve", ["address", "uint256"], "bool", mutability="nonpayable"),
]


class RpcTransportError(RuntimeError):
    pass


class _RedactingLogger(logging.LoggerAdapter):
    """Keep provider endpoint and header credentials out of transport logs."""

    def __init__(self, wrapped, secret):
        super().__init__(wrapped, {})
        self._secret = secret

    def log(self, level, msg, *args, **kwargs):
        if self.isEnabledFor(level):
            msg = self._redact(msg)
            args = tuple(self._redact(value) for value in args)
            self.logger.log(level, msg, *args, **kwargs)

    def _redact(self, value):
        if isinstance(value, str):
            return value.replace(self._secret, "[redacted]")
        return value


class AsyncFallbackProvider(AsyncBaseProvider):
    def __init__(self, primary, fallback, chain_name):
        super().__init__()
        self.primary = primary
        self.fallback = fallback
        self.chain_name = chain_name

    async def make_request(self, method, params):
        try:
            return await self.primary.make_request(method, params)
        except Exception:  # noqa: BLE001 - AsyncHTTPProvider exceptions define transport failure
            if self.fallback is None:
                raise RpcTransportError(f"{self.chain_name} Ankr transport failed") from None
            logger.warning("%s Ankr transport failed; using dRPC", self.chain_name)
        try:
            return await self.fallback.make_request(method, params)
        except Exception:  # noqa: BLE001 - sanitize every dRPC provider failure at this boundary
            raise RpcTransportError(f"{self.chain_name} dRPC transport failed") from None

    async def is_connected(self, show_traceback=False):
        try:
            response = await self.make_request("web3_clientVersion", [])
        except RpcTransportError:
            if show_traceback:
                raise
            return False
        connected = response.get("jsonrpc") == "2.0" and "error" not in response
        if show_traceback and not connected:
            raise RpcTransportError(f"{self.chain_name} RPC connectivity check failed")
        return connected

    async def disconnect(self):
        try:
            await self.primary.disconnect()
        finally:
            if self.fallback is not None:
                await self.fallback.disconnect()


def create_web3(chain, environ):
    ankr_api_key = environ.get("ANKR_API_KEY", "").strip()
    if not ankr_api_key:
        raise ValueError("ANKR_API_KEY is required")

    # Disable provider-local retries so one Ankr failure reaches dRPC immediately.
    primary = AsyncHTTPProvider(
        f"https://rpc.ankr.com/{chain['ankr_slug']}/{ankr_api_key}",
        exception_retry_configuration=None,
    )
    primary.logger = _RedactingLogger(primary.logger, ankr_api_key)

    fallback = None
    drpc_api_key = environ.get("DRPC_API_KEY", "").strip()
    if drpc_api_key:
        fallback = AsyncHTTPProvider(
            f"https://lb.drpc.live/{chain['drpc_slug']}/",
            request_kwargs={
                "headers": {
                    "Content-Type": "application/json",
                    "Drpc-Key": drpc_api_key,
                }
            },
            exception_retry_configuration=None,
        )
        fallback.logger = _RedactingLogger(fallback.logger, drpc_api_key)

    return AsyncWeb3(AsyncFallbackProvider(primary, fallback, chain["name"]))


GWEI = 10**9


class TransactionError(RuntimeError):
    pass


class NoIntentSurvived(TransactionError):
    pass


class TransactionLane:
    def __init__(self, app, chain, environ):
        key = environ.get("KEEPER_EOA_PK", "").strip()
        if not key:
            raise TransactionError("KEEPER_EOA_PK is required")
        self.app, self.chain = app, chain
        try:
            self.w3 = create_web3(chain, environ)
        except ValueError as exc:
            raise TransactionError(str(exc)) from None
        self.account = Account.from_key(key)
        self.address = self.account.address
        self.active_batch = []
        self._lock = asyncio.Lock()

    async def initialize(self):
        if await self.w3.eth.chain_id != self.chain["chain_id"]:
            raise TransactionError(f"{self.chain['name']} RPC chain mismatch")

    async def start_batch(self, intents):
        """Number an in-memory batch from the latest mined nonce, then broadcast it."""
        if not intents:
            return
        async with self._lock:
            if self.active_batch:
                raise TransactionError(f"{self.chain['name']} already has an active batch")

            estimates = await asyncio.gather(*(self._estimate_intent(intent) for intent in intents))
            estimated_intents = [estimate for estimate in estimates if estimate is not None]
            if not estimated_intents:
                return

            (max_fee, priority_fee), nonce, balance = await asyncio.gather(
                self._quote_fees(),
                self.w3.eth.get_transaction_count(self.address, "latest"),
                self.w3.eth.get_balance(self.address),
            )
            batch = [
                {
                    **intent,
                    "nonce": nonce + offset,
                    "value": intent.get("value", 0),
                    "gas": (gas * self.app["gas_limit_bps"] + 9_999) // 10_000,
                    "max_fee": max_fee,
                    "tip": priority_fee,
                    "hashes": [],
                }
                for offset, (intent, gas) in enumerate(estimated_intents)
            ]
            cost = sum(item["value"] + item["gas"] * item["max_fee"] for item in batch)
            if cost > balance:
                raise TransactionError(f"{self.chain['name']} batch exceeds EOA balance")

            self.active_batch = batch
            for item in self.active_batch:
                try:
                    await self._broadcast_version(item)
                    logger.info(
                        "%s submitted %s nonce=%d tx=%s",
                        self.chain["name"],
                        item["label"],
                        item["nonce"],
                        item["hashes"][-1],
                    )
                except Exception:
                    logger.exception("%s stopped at nonce %d", self.chain["name"], item["nonce"])
                    break

    async def finish_active_batch(self):
        """Wait for this process's active batch, checking inclusion at each pending interval."""
        mined = await self.reconcile()
        while self.active_batch:
            wait_item = next((item for item in reversed(self.active_batch) if item["hashes"]), None)
            if wait_item is not None:
                timeout = self.app["pending_poll_seconds"]
                logger.info(
                    "%s waiting up to %ds for nonce=%d receipt",
                    self.chain["name"],
                    timeout,
                    wait_item["nonce"],
                )
                try:
                    # Inclusion of the highest broadcast nonce also includes every prior nonce.
                    await self.w3.eth.wait_for_transaction_receipt(
                        wait_item["hashes"][-1],
                        timeout=timeout,
                        poll_latency=min(1, timeout),
                    )
                except TimeExhausted:
                    pass
            else:
                # A failed initial broadcast has no receipt to await; retain the same backoff.
                await asyncio.sleep(self.app["pending_poll_seconds"])
            mined.extend(await self.reconcile())
        mined.sort(key=lambda item: item["nonce"])
        return mined

    async def submit_and_wait(self, intents):
        """Finish this process's active batch, then submit and wait for a new one."""
        prior = await self.finish_active_batch()
        reverted = next((item for item in prior if item["status"] != 1), None)
        if reverted is not None:
            raise TransactionError(
                f"{self.chain['name']} transaction {reverted['label']} at nonce "
                f"{reverted['nonce']} reverted ({reverted['tx_hash']})"
            )

        intents = list(intents)
        if not intents:
            raise NoIntentSurvived(f"{self.chain['name']} no intent survived gas estimation")
        maximum_batch_size = self.chain["max_batch_size"]
        if len(intents) > maximum_batch_size:
            raise TransactionError(
                f"{self.chain['name']} batch exceeds maximum of {maximum_batch_size} intents"
            )

        await self.start_batch(intents)
        if not self.active_batch:
            raise NoIntentSurvived(f"{self.chain['name']} no intent survived gas estimation")
        return await self.finish_active_batch()

    async def reconcile(self):
        """Check receipts for this process's transaction hashes."""
        async with self._lock:
            mined, unresolved = await self._reconcile_receipts(self.active_batch)
            mined.sort(key=lambda item: item["nonce"])
            self.active_batch = unresolved
            return mined

    async def _estimate_intent(self, intent):
        try:
            gas = await self.w3.eth.estimate_gas(
                {
                    "from": self.address,
                    "to": intent["to"],
                    "data": intent["data"],
                    "value": intent.get("value", 0),
                }
            )
        except Exception as exc:  # noqa: BLE001 - isolation boundary: reject intent, keep going
            logger.error("%s rejected intent %r: %s", self.chain["name"], intent, exc)
            return None
        return intent, gas

    async def _reconcile_receipts(self, items):
        mined, pending = [], []
        for item in items:
            receipt = None
            for tx_hash in reversed(item["hashes"]):
                try:
                    receipt = await self.w3.eth.get_transaction_receipt(tx_hash)
                    break
                except TransactionNotFound:
                    pass
            if receipt is None:
                pending.append(item)
                continue
            item["status"], item["tx_hash"] = receipt["status"], tx_hash
            mined.append(item)
            logger.info(
                "%s included %s nonce=%d status=%d tx=%s",
                self.chain["name"],
                item["label"],
                item["nonce"],
                item["status"],
                tx_hash,
            )
        return mined, pending

    async def _quote_fees(self):
        block, node_tip = await asyncio.gather(
            self.w3.eth.get_block("latest"),
            self.w3.eth.max_priority_fee,
        )
        base_fee = block.get("baseFeePerGas") or 0
        if base_fee > self.chain["max_base_fee_gwei"] * GWEI:
            raise TransactionError(f"{self.chain['name']} base fee cap reached")
        if base_fee:
            priority_fee = max(5 * base_fee // 100, node_tip, 1)
            return max(2 * base_fee, base_fee + priority_fee), priority_fee
        gas_price = await self.w3.eth.gas_price
        priority_fee = max(gas_price, node_tip, 1)
        return priority_fee, priority_fee

    async def _broadcast_version(self, item):
        signed = self.account.sign_transaction(
            {
                "type": 2,
                "chainId": self.chain["chain_id"],
                "nonce": item["nonce"],
                "to": item["to"],
                "data": item["data"],
                "value": item["value"],
                "gas": item["gas"],
                "maxFeePerGas": item["max_fee"],
                "maxPriorityFeePerGas": item["tip"],
            }
        )
        tx_hash = Web3.to_hex(signed.hash)
        if tx_hash not in item["hashes"]:
            item["hashes"].append(tx_hash)
        try:
            await self.w3.eth.send_raw_transaction(signed.raw_transaction)
        except Exception as exc:
            if "already known" not in str(exc).lower():
                raise


class SkipPool(RuntimeError):
    pass


class Pool:
    def __init__(self, config, w3, slippage_bps):
        self.id = config["id"]
        self.address = config["address"]
        self.upkeep_interval_seconds = config["frequency_seconds"]
        self.target_coin_index = config["target_coin_idx"]
        self.target_coin_amount = config["target_coin_amt"]
        self.w3 = w3
        self.slippage_bps = slippage_bps
        self.pool_contract = w3.eth.contract(address=self.address, abi=TWOCRYPTO_POOL_ABI)
        self.coin_addresses = None
        self.token_contracts = None
        self.input_amounts = None

    async def initialize(self):
        coin0, coin1 = await asyncio.gather(
            self.pool_contract.functions.coins(0).call(),
            self.pool_contract.functions.coins(1).call(),
        )
        coin_addresses = tuple(map(Web3.to_checksum_address, (coin0, coin1)))
        token_contracts = tuple(
            self.w3.eth.contract(address=coin, abi=ERC20_ABI) for coin in coin_addresses
        )
        if coin_addresses[0] == coin_addresses[1]:
            raise SkipPool("invalid pool")

        target_token = token_contracts[self.target_coin_index]
        target_input_amount = (
            Decimal(str(self.target_coin_amount))
            * 10 ** await target_token.functions.decimals().call()
        )
        if target_input_amount != target_input_amount.to_integral_value():
            raise SkipPool("amount exceeds token precision")
        target_input_amount = int(target_input_amount)
        paired_input_amount = await self.pool_contract.functions.get_dy(
            self.target_coin_index,
            1 - self.target_coin_index,
            target_input_amount,
        ).call()
        if not target_input_amount or not paired_input_amount:
            raise SkipPool("zero quote")

        input_amounts = [paired_input_amount, paired_input_amount]
        input_amounts[self.target_coin_index] = target_input_amount
        self.coin_addresses, self.token_contracts, self.input_amounts = (
            coin_addresses,
            token_contracts,
            tuple(input_amounts),
        )

    async def fetch_last_upkeep_timestamp(self, block="latest"):
        return await self.pool_contract.functions.last_timestamp().call(block_identifier=block)

    async def _select_input(self, owner_address, balance_limits):
        onchain_balances = await asyncio.gather(
            *(token.functions.balanceOf(owner_address).call() for token in self.token_contracts)
        )
        balances = tuple(
            min(balance, balance_limits.get(address, balance))
            for address, balance in zip(self.coin_addresses, onchain_balances, strict=True)
        )
        preferred = (self.target_coin_index, 1 - self.target_coin_index)

        # Prefer a complete configured-side swap, then a complete reverse-side swap.
        for input_coin_index in preferred:
            if balances[input_coin_index] >= self.input_amounts[input_coin_index]:
                input_amount = self.input_amounts[input_coin_index]
                return input_coin_index, input_amount, balances[input_coin_index] - input_amount

        # If neither side is fully funded, retain the bounded whole-balance fallback.
        for input_coin_index in preferred:
            if balances[input_coin_index] * 10 > self.input_amounts[input_coin_index]:
                return input_coin_index, balances[input_coin_index], 0

        raise SkipPool("insufficient inventory on both sides")

    async def prepare_intent(self, owner_address, balance_limits):
        input_coin_index, input_amount, remaining_balance = await self._select_input(
            owner_address, balance_limits
        )
        quote = await self.pool_contract.functions.get_dy(
            input_coin_index, 1 - input_coin_index, input_amount
        ).call()
        exchange_args = [
            input_coin_index,
            1 - input_coin_index,
            input_amount,
            max(1, quote * (10_000 - self.slippage_bps) // 10_000),
            owner_address,
        ]
        intent = {
            "label": f"{self.id}:swap",
            "to": self.address,
            "data": self.pool_contract.encode_abi("exchange", args=exchange_args),
        }
        return intent, self.coin_addresses[input_coin_index], remaining_balance
