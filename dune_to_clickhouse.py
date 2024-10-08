import os
import requests
import time
from dotenv import load_dotenv
import clickhouse_connect
from dune_client.types import QueryParameter
from dune_client.client import DuneClient
from dune_client.query import QueryBase

# Load environment variables
load_dotenv()

# Dune Analytics API configuration
DUNE_API_KEY = os.getenv('DUNE_API_KEY')
DUNE_API_BASE_URL = 'https://api.dune.com/api/v1'

# ClickHouse configuration
CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST')
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD')
CLICKHOUSE_DATABASE_USERS_DEFI = os.getenv('CLICKHOUSE_DATABASE_USERS_DEFI')
CLICKHOUSE_DATABASE_USERS_TOKENS = os.getenv('CLICKHOUSE_DATABASE_USERS_TOKENS')

def read_addresses(file_path):
    with open(file_path, 'r') as f:
        return [line.strip() for line in f]

def execute_dune_query(query_id, addresses):
    query = QueryBase(
        name="Wallet Analysis Query",
        query_id=query_id,
        params=[
            QueryParameter.text_type(name="addresses", value=",".join(addresses)),
        ],
    )

    dune = DuneClient.from_env()
    results = dune.run_query_dataframe(query)
    
    return results.to_dict('records')

def create_clickhouse_tables(client):
    client.command("""
    CREATE TABLE IF NOT EXISTS token_balances (
        wallet_address String,
        blockchain String,
        token_address String,
        total_balance Float64,
        token_symbol String,
        usd_price Float64,
        usd_value Float64,
        price_date DateTime
    ) ENGINE = MergeTree()
    ORDER BY (wallet_address, blockchain, token_address)
    """)

    client.command("""
    CREATE TABLE IF NOT EXISTS defi_trades (
        wallet_address String,
        defi_protocol String,
        trade_count UInt32
    ) ENGINE = MergeTree()
    ORDER BY (wallet_address, defi_protocol)
    """)

def insert_token_balances(client, data):
    client.insert('token_balances', data)

def insert_defi_trades(client, data):
    client.insert('defi_trades', data)

def main():
    addresses = read_addresses('addresses_head.csv')
    
    # Execute Dune queries
    token_balances = execute_dune_query('4140317', addresses)
    defi_trades = execute_dune_query('4140137', addresses)
    
    # Connect to ClickHouse
    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE_USERS_TOKENS,
        secure=True
    )
    
    # Create tables if they don't exist
    create_clickhouse_tables(client)
    
    # Insert data into ClickHouse
    insert_token_balances(client, token_balances)
    
    # Switch database for defi_trades
    client.command(f"USE {CLICKHOUSE_DATABASE_USERS_DEFI}")
    insert_defi_trades(client, defi_trades)
    
    print("Data successfully inserted into ClickHouse tables.")

if __name__ == "__main__":
    main()