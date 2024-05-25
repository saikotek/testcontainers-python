import pytest
from azure.cosmos import PartitionKey, CosmosClient
import urllib3

from testcontainers.cosmosdb import CosmosDbContainer


def test_docker_run_cosmosdb():
    urllib3.disable_warnings()
    with CosmosDbContainer("mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator:latest", ssl_verify=False) as cosmosdb:
        client = cosmosdb.get_connection_client()

        database = client.create_database_if_not_exists(
            id="cosmicworks",
            offer_throughput=400,
        )

        partition_key = PartitionKey(
            path="/id",
        )

        container = database.create_container_if_not_exists(
            id="products",
            partition_key=partition_key,
        )

        new_item = {
            "id": "70b63682-b93a-4c77-aad2-65501347265f",
            "category": "gear-surf-surfboards",
            "name": "Yamba Surfboard",
            "quantity": 12,
            "sale": False,
        }

        created_item = container.upsert_item(new_item)
        existing_item = container.read_item(
            item=new_item["id"],
            partition_key=new_item["id"]
        )

        assert created_item == existing_item