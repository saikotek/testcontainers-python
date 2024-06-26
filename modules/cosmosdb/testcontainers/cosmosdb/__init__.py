#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.
import os
import socket
import time
from typing import Optional

import requests
from azure.cosmos import CosmosClient

from testcontainers.core.generic import DbContainer
from testcontainers.core.utils import setup_logger
from testcontainers.core.waiting_utils import wait_for_logs

logger = setup_logger(__name__)


class CosmosDbContainer(DbContainer):
    """
    Azure CosmosDb emulator container.
    Is it important to understand the limitations of the emulator:
    https://learn.microsoft.com/en-us/azure/cosmos-db/emulator

    Notably, you cannot change the default account endpoint and account key.
    Currently, SSL verification is not supported.

    Example:

        The example will spin up a CosmosDb emulator, connect to it,
        create a database and a container.

        .. doctest::

            >>> from azure.cosmos import PartitionKey
            >>> import urllib3
            >>> from testcontainers.cosmosdb import CosmosDbContainer
            >>> urllib3.disable_warnings()
            >>> with CosmosDbContainer() as cosmosdb:
            ...     client = cosmosdb.get_connection_client()
            ...     database = client.create_database_if_not_exists(
            ...         id="cosmicworks",
            ...         offer_throughput=400,
            ...     )
            ...     partition_key = PartitionKey(
            ...         path="/id",
            ...     )
            ...     container = database.create_container_if_not_exists(
            ...         id="products",
            ...         partition_key=partition_key,
            ...     )
    """

    localhost = "localhost"
    ip_address = socket.gethostbyname(localhost)
    port = 8081
    timeout = 120.0

    def __init__(
        self,
        image: str = "mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator:latest",
        partition_count: Optional[str] = None,
        ip_address_override: Optional[str] = None,
        **kwargs,
    ) -> None:
        if "port" in kwargs:
            logger.warn(
                "Port is specified in kwargs, but it is not supported by CosmosDb emulator. \
                        Default port {self.PORT} will be used."
            )

        super().__init__(image=image, **kwargs)

        self.partition_count = (
            partition_count if partition_count else os.environ.get("AZURE_COSMOS_EMULATOR_PARTITION_COUNT", "")
        )
        self.ip_address = ip_address_override if ip_address_override else self.ip_address
        self.with_bind_ports(self.port, self.port)
        for p in range(10250, 10256):
            self.with_bind_ports(p, p)

    def _configure(self) -> None:
        if self.partition_count:
            self.with_env("AZURE_COSMOS_EMULATOR_PARTITION_COUNT", self.partition_count)
        self.with_env("AZURE_COSMOS_EMULATOR_IP_ADDRESS_OVERRIDE", self.ip_address)

    @staticmethod
    def get_account_key() -> str:
        # This is a static key
        return "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw=="

    def get_connection_url(self) -> str:
        return f"https://{self.localhost}:{self.port}/"

    def _connect(self) -> None:
        def check_logs(stdout: str) -> bool:
            return stdout.splitlines()[-1].endswith("Started") if stdout else False

        logger.info("Waiting for started marker in logs...")
        wait_for_logs(self, check_logs, timeout=self.timeout)

        def wait_for_successful_request() -> bool:
            try:
                response = requests.get(f"{self.get_connection_url()}_explorer/emulator.pem", verify=False)
                return response.status_code == 200
            except requests.exceptions.RequestException:
                return False

        start_time = time.time()
        logger.info("Waiting for endpoint to be available...")
        while True:
            duration = time.time() - start_time
            if duration > self.timeout:
                raise TimeoutError("Container did not start in time")
            if wait_for_successful_request():
                break
            time.sleep(1)

    def start(self) -> "CosmosDbContainer":
        super().start()
        self._connect()
        return self

    def get_connection_client(self) -> CosmosClient:
        return CosmosClient(
            self.get_connection_url(),
            credential=CosmosDbContainer.get_account_key(),
            connection_verify=False,
            retry_total=3,
        )
