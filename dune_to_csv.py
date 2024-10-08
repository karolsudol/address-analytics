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

def write_to_csv(data, filename):
    if not data:
        print(f"No data to write to {filename}")
        return

    fieldnames = data[0].keys()
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in data:
            writer.writerow(row)
    
    print(f"Data successfully written to {filename}")

def main():
    addresses = read_addresses('addresses_head.csv')
    
    # Execute Dune queries
    token_balances = execute_dune_query('4140317', addresses)
    defi_trades = execute_dune_query('4140137', addresses)
    
    # Write data to CSV files
    write_to_csv(token_balances, 'token_balances.csv')
    write_to_csv(defi_trades, 'defi_trades.csv')

if __name__ == "__main__":
    main()