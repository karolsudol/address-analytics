import os
import csv
from dotenv import load_dotenv
from dune_client.types import QueryParameter
from dune_client.client import DuneClient
from dune_client.query import QueryBase

# Load environment variables
load_dotenv()

# Dune Analytics API configuration
DUNE_API_KEY = os.getenv('DUNE_API_KEY')

def read_addresses(file_path):
    print(f"Reading addresses from {file_path}...")
    with open(file_path, 'r') as f:
        addresses = [line.strip() for line in f]
    print(f"Read {len(addresses)} addresses.")
    return addresses

def execute_dune_query(query_id, addresses):
    print(f"Executing Dune query {query_id} for {len(addresses)} addresses...")
    query = QueryBase(
        name="Wallet Analysis Query",
        query_id=query_id,
        params=[
            QueryParameter.text_type(name="addresses", value=",".join(addresses)),
        ],
    )

    dune = DuneClient.from_env()
    results = dune.run_query_dataframe(query)
    
    print(f"Query {query_id} executed successfully. Retrieved {len(results)} rows.")
    return results.to_dict('records')

def write_to_csv(data, filename, mode='w'):
    if not data:
        print(f"No data to write to {filename}")
        return

    print(f"Writing {len(data)} rows to {filename}...")
    fieldnames = data[0].keys()
    with open(filename, mode, newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if mode == 'w':
            writer.writeheader()
        for row in data:
            writer.writerow(row)
    
    print(f"Data successfully written to {filename}")

def process_token_balances(addresses, batch_size=50):
    print("Processing token balances...")
    for i in range(0, len(addresses), batch_size):
        batch = addresses[i:i+batch_size]
        token_balances = execute_dune_query('4140317', batch)
        write_to_csv(token_balances, 'token_balances.csv', 'a' if i > 0 else 'w')

def process_defi_trades(addresses, batch_size=50):
    print("Processing DeFi trades...")
    for i in range(0, len(addresses), batch_size):
        batch = addresses[i:i+batch_size]
        defi_trades = execute_dune_query('4140137', batch)
        write_to_csv(defi_trades, 'defi_trades.csv', 'a' if i > 0 else 'w')

def main():
    addresses = read_addresses('addresses.csv')
    
    print("Starting Dune Analytics data extraction...")

    # process_token_balances(addresses)
    process_defi_trades(addresses)
    
    print("Data extraction and CSV writing completed successfully.")

if __name__ == "__main__":
    main()