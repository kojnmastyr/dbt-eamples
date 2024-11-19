import requests
from threading import Lock
from concurrent.futures import ThreadPoolExecutor, wait
from typing import List, Dict, Optional, Any
import codecs
import datetime
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class OmniAPIClient:
    _ENDPOINT = (
        os.getenv("RPC_ENDPOINT") or "http://127.0.0.1:8332"
    )  # TODO: Use AWS Secret Manager if available or.env
    _USERNAME = (
        os.getenv("RPC_USERNAME") or "root"
    )  # TODO: Use AWS Secret Manager if available or.env
    _PASSWORD = (
        os.getenv("RPC_PASSWORD") or "bitcoin"
    )  # TODO: Use AWS Secret Manager if available or .env
    _HEADERS = {"content-type": "application/json"}

    def __init__(
        self,
        endpoint: str = _ENDPOINT,
        username: str = _USERNAME,
        password: str = _PASSWORD,
    ):
        self.endpoint = endpoint
        self.auth = (username, password)
        self.headers = self._HEADERS

    def perform_request(self, method: str, params: List[Any] = []) -> Any:
        payload = {
            "jsonrpc": "2.0",
            "id": "curlrpc",
            "method": method,
            "params": params,
        }
        resp = requests.post(
            self.endpoint, auth=self.auth, headers=self.headers, json=payload
        )
        resp.raise_for_status()

        response = resp.json()
        if "error" in response and response["error"]:
            raise ValueError(f"API Error: {response['error']}")
        return response["result"]

    def get_transaction(self, tx: str) -> Dict[str, Any]:
        return self.perform_request("omni_gettransaction", [tx])

    def get_info(self) -> Dict[str, Any]:
        return self.perform_request("omni_getinfo")

    def list_wallet_transactions(
        self, address: str, count: int = 10, skip: int = 0
    ) -> Dict[str, Any]:
        return self.perform_request("omni_listtransactions", [address, count, skip])

    def list_blocks_transactions(
        self, block_start: int, block_end: int
    ) -> Dict[str, Any]:
        return self.perform_request(
            "omni_listblockstransactions", [block_start, block_end]
        )

    def send_requests(self, method: str, param_list: List[List[Any]]) -> List[Any]:
        with ThreadPoolExecutor(max_workers=2) as executor:
            lock, futures = Lock(), [
                executor.submit(self.perform_request, method, params)
                for params in param_list
            ]

            while futures:
                done, futures = wait(futures, return_when="FIRST_COMPLETED")
                for future in done:
                    with lock:
                        if future.exception():
                            if hasattr(future, "retries") and future.retries > 0:
                                futures.append(
                                    executor.submit(
                                        self.perform_request, method, future.params
                                    )
                                )
                                future.retries -= 1
                                logger.warning(
                                    f"Retrying due to error: {future.exception()}"
                                )
                            else:
                                logger.error(
                                    f"Retries exhausted with error: {future.exception()}"
                                )
                        else:
                            yield future.result()

    def decode_transaction_hex(
        self, tx_list: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        for tx in tx_list:
            try:
                tx["decoded_data"] = codecs.decode(tx["data"], "hex").decode("utf-8")
                if "blocktime" in tx:
                    block_datetime = datetime.datetime.utcfromtimestamp(
                        tx["blocktime"]
                    ).isoformat()
                    tx["blockdate"] = block_datetime
            except (UnicodeDecodeError, TypeError) as e:
                tx["decoded_data"] = None
                logger.error(f"Failed to decode transaction: {e}")
        return tx_list

    def get_embed_data_transactions(
        self, start_block: int, end_block: int
    ) -> List[Dict[str, Any]]:
        transactions = self.list_blocks_transactions(start_block, end_block)
        tx_details = list(
            self.send_requests("omni_gettransaction", [[tx] for tx in transactions])
        )
        data_txns = [
            tx for tx in tx_details if tx.get("type_int") == 200 and "data" in tx
        ]
        return self.decode_transaction_hex(data_txns)
