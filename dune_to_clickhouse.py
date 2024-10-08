import os
import requests
from dotenv import load_dotenv
from clickhouse_driver import Client

# Load environment variables
load_dotenv()

# Dune Analytics API configuration
DUNE_API_KEY = os.getenv('DUNE_API_KEY')
DUNE_API_BASE_URL = 'https://api.dune.com/api/v1'

# ClickHouse configuration
CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST')
CLICKHOUSE_PORT = os.getenv('CLICKHOUSE_PORT')
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD')
CLICKHOUSE_DATABASE = os.getenv('CLICKHOUSE_DATABASE')

def read_addresses(file_path):
    with open(file_path, 'r') as f:
        return [line.strip() for line in f]

def execute_dune_query(query_id, addresses):
    url = f"{DUNE_API_BASE_URL}/query/{query_id}/execute"
    params = {
        "query_parameters": {
            "addresses": ",".join(addresses)
        }
    }
    headers = {"X-Dune-API-Key": DUNE_API_KEY}
    
    response = requests.post(url, json=params, headers=headers)
    response.raise_for_status()
    
    execution_id = response.json()['execution_id']
    
    # Poll for results
    while True:
        status_url = f"{DUNE_API_BASE_URL}/execution/{execution_id}/status"
        status_response = requests.get(status_url, headers=headers)
        status_response.raise_for_status()
        
        if status_response.json()['state'] == 'QUERY_STATE_COMPLETED':
            results_url = f"{DUNE_API_BASE_URL}/execution/{execution_id}/results"
            results_response = requests.get(results_url, headers=headers)
            results_response.raise_for_status()
            return results_response.json()['result']['rows']
        
        time.sleep(5)  # Wait 5 seconds before polling again

def create_clickhouse_tables(client):
    client.execute("""
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

    client.execute("""
    CREATE TABLE IF NOT EXISTS defi_trades (
        wallet_address String,
        defi_protocol String,
        trade_count UInt32
    ) ENGINE = MergeTree()
    ORDER BY (wallet_address, defi_protocol)
    """)

def insert_token_balances(client, data):
    client.execute(
        "INSERT INTO token_balances VALUES",
        data
    )

def insert_defi_trades(client, data):
    client.execute(
        "INSERT INTO defi_trades VALUES",
        data
    )

def main():
    addresses = read_addresses('addresses.csv')
    
    # Execute Dune queries
    token_balances = execute_dune_query('4140317', addresses)
    defi_trades = execute_dune_query('4140137', addresses)
    
    # Connect to ClickHouse
    client = Client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE
    )
    
    # Create tables if they don't exist
    create_clickhouse_tables(client)
    
    # Insert data into ClickHouse
    insert_token_balances(client, token_balances)
    insert_defi_trades(client, defi_trades)
    
    print("Data successfully inserted into ClickHouse tables.")

if __name__ == "__main__":
    main()