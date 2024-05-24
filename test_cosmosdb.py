import pytest
from pymongo import MongoClient
from pymongo.errors import OperationFailure
from azure.cosmos import CosmosClient, PartitionKey

from modules.cosmosdb.testcontainers.cosmosdb import CosmosDbContainer
import urllib3

def test_docker_run_cosmosdb():
    urllib3.disable_warnings()
    
    client = CosmosClient(
            "https://localhost:8081",
            credential=CosmosDbContainer.get_account_key(),
            connection_verify=False,
        )
    
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