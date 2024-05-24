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
from typing import Optional

from azure.cosmos import CosmosClient

from testcontainers.core.generic import DbContainer
from testcontainers.core.utils import raise_for_deprecated_parameter
from testcontainers.core.waiting_utils import wait_for_logs


class CosmosDbContainer(DbContainer):
    """
    Azure CosmosDb emulator container.
    """
    # IP_ADDRESS = socket.gethostbyname(socket.gethostname())
    
    def __init__(
        self,
        image: str = "mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator:latest",
        port: int = 8081,
        ssl_verify: bool = False,
        partition_count: Optional[str] = None,
        ip_address_override: Optional[str] = None,
        **kwargs,
    ) -> None:
        raise_for_deprecated_parameter(kwargs, "port_to_expose", "port")
        super().__init__(image=image, **kwargs)
        self.ssl_verify = ssl_verify
        self.port = port
        self.partition_count = partition_count if partition_count \
            else os.environ.get("AZURE_COSMOS_EMULATOR_PARTITION_COUNT", "")
        self.ip_address_override = ip_address_override if ip_address_override \
            else os.environ.get("AZURE_COSMOS_EMULATOR_IP_ADDRESS_OVERRIDE", "")
        self.with_exposed_ports(self.port)
        # self.with_bind_ports(self.port, self.port)
        # self.with_bind_ports(8081, 8081)
        for p in range(10250, 10256):
            self.with_bind_ports(p, p)
        # self._configure()

    def _configure(self) -> None:
        if self.partition_count:
            self.with_env("AZURE_COSMOS_EMULATOR_PARTITION_COUNT", self.partition_count)
        if self.ip_address_override:
            self.with_env("AZURE_COSMOS_EMULATOR_IP_ADDRESS_OVERRIDE", self.ip_address_override)
        # self.with_env("IP_ADDRESS", self.IP_ADDRESS)
    
    @staticmethod
    def get_account_key() -> str:
        # This is a static key
        return "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw=="
    
    def get_connection_url(self) -> str:
        return f"https://{self.get_container_host_ip()}:{self.get_exposed_port(self.port)}"
    
    def _connect(self) -> None:
        def check_logs(stdout: str) -> bool:
            if stdout:
                lines = stdout.splitlines()
                if len(lines) > 0:
                    return lines[-1].endswith("Started")
            return False
        wait_for_logs(self, check_logs, timeout = 120.0)
    
    def get_connection_client(self) -> CosmosClient:
        print(f"Connection URL: {self.get_connection_url()}")
        return CosmosClient(
            self.get_connection_url(),
            credential=CosmosDbContainer.get_account_key(),
            connection_verify=False,
        )
    
