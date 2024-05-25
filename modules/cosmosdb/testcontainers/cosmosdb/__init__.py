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
import requests
from typing import Optional

from azure.cosmos import CosmosClient

from testcontainers.core.generic import DbContainer
from testcontainers.core.utils import raise_for_deprecated_parameter, setup_logger
from testcontainers.core.waiting_utils import wait_for_logs

logger = setup_logger(__name__)

class CosmosDbContainer(DbContainer):
    """
    Azure CosmosDb emulator container.
    """
    LOCALHOST = "localhost"
    IP_ADDRESS = socket.gethostbyname(LOCALHOST)
    PORT = 8081

    def __init__(
        self,
        image: str = "mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator:latest",
        ssl_verify: bool = False,
        partition_count: Optional[str] = None,
        ip_address_override: Optional[str] = None,
        **kwargs,
    ) -> None:
        raise_for_deprecated_parameter(kwargs, "port_to_expose", "port")
        super().__init__(image=image, **kwargs)
        self.ssl_verify = ssl_verify
        self.partition_count = partition_count if partition_count \
            else os.environ.get("AZURE_COSMOS_EMULATOR_PARTITION_COUNT", "")
        self.ip_address_override = ip_address_override if ip_address_override \
            else self.IP_ADDRESS
        self.with_bind_ports(self.PORT, self.PORT)
        for p in range(10250, 10256):
            self.with_bind_ports(p, p)

    def _configure(self) -> None:
        if self.partition_count:
            self.with_env("AZURE_COSMOS_EMULATOR_PARTITION_COUNT", self.partition_count)
        self.with_env("AZURE_COSMOS_EMULATOR_IP_ADDRESS_OVERRIDE", self.IP_ADDRESS)

    @staticmethod
    def get_account_key() -> str:
        # This is a static key
        return "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw=="

    def get_connection_url(self) -> str:
        return f"https://{self.LOCALHOST}:{self.PORT}/"

    def _connect(self) -> None:
        def check_logs(stdout: str) -> bool:
            return stdout.splitlines()[-1].endswith("Started") if stdout else False

        logger.info("Waiting for logs started marker ...")
        wait_for_logs(self, check_logs, timeout=120.0)
        logger.info("Logs started marker found")

        def wait_for_successful_request() -> bool:
            try:
                response = requests.get(
                    f"{self.get_connection_url()}_explorer/emulator.pem", verify=False)
                logger.info(f"Response status code: {response.status_code}")
                return response.status_code == 200
            except requests.exceptions.RequestException:
                logger.info("Request failed")
                return False

        start_time = time.time()
        limit = 120.0
        while True:
            duration = time.time() - start_time
            if duration > limit:
                raise TimeoutError("Container did not start in time")
            if wait_for_successful_request():
                break
            time.sleep(1)
            logger.info(f"Waiting for CosmosDb container to start ... {duration:.3f} seconds")

    def start(self) -> "CosmosDbContainer":
        super().start()
        self._connect()
        return self

    def get_connection_client(self) -> CosmosClient:
        logger.info(f"Connection URL: {self.get_connection_url()}")
        return CosmosClient(
            self.get_connection_url(),
            credential=CosmosDbContainer.get_account_key(),
            connection_verify=False,
            retry_total=3
        )
