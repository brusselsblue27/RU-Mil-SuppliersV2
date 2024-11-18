import os
import pandas as pd
import requests
import time
import logging
from dotenv import load_dotenv

# Load environment variables from a .env file (if present)
load_dotenv()

# Setup Logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/app_debug.log"),
        logging.StreamHandler()
    ]
)

# Directories and file paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, 'output')
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Configurations
DATE_RANGE = ("2014-07-31", "2022-02-23")
TEST_MODE = False  # Set this to True to limit to 20 companies for testing


def is_valid_inn(number):
    """Check if the given number is a valid INN format (10 or 12 digits)."""
    return isinstance(number, str) and number.isdigit() and len(number) in [10, 12]


def get_api_keys():
    """Retrieve API keys from environment variables or prompt user if not found."""
    opensanctions_key = os.getenv("OPENSANCTIONS_API_KEY")
    clearspending_keys = [
        os.getenv(f"CLEARSPENDING_API_KEY_{i+1}") for i in range(9)
    ]
    
    if not opensanctions_key:
        opensanctions_key = input("Enter your OpenSanctions API key: ")
    
    for i in range(9):
        if not clearspending_keys[i]:
            clearspending_keys[i] = input(f"Enter ClearSpending API key {i+1}: ")
    
    clearspending_keys = [key for key in clearspending_keys if key]
    return opensanctions_key, clearspending_keys


def setup_mode():
    """Set mode and keyword preferences, allowing the user to define excluded keywords."""
    global TEST_MODE
    mode = input("Run in test mode? (y/n): ").lower()
    TEST_MODE = mode == 'y'

    manual_inn_mode = input("Do you want to manually enter INN numbers and skip OpenSanctions? (y/n): ").lower()
    if manual_inn_mode == 'y':
        TEST_MODE = False  # Disable test mode for INN-only searches
        logging.warning("Test mode is not available for INN-only search. Running in full mode.")
        inn_list = input("Enter INN numbers separated by commas: ").split(',')
        inn_list = [inn.strip() for inn in inn_list if is_valid_inn(inn.strip())]
        product_codes = input("Enter product codes (OKPD2) for filtering ClearSpending data (comma-separated): ").split(',')
        logging.info(f"Manual INN mode enabled with INNs: {inn_list} and OKPD2 filters: {product_codes}")
        return None, None, product_codes, inn_list

    keywords = input("Enter keywords for search (comma-separated): ").split(',')
    default_exclusions = ["banks", "politics", "medical"]
    user_exclusions = input(f"Enter keywords to exclude (default: {', '.join(default_exclusions)}): ")
    excluded_keywords = user_exclusions.split(',') if user_exclusions else default_exclusions

    product_codes = input("Enter product codes (OKPD2) for filtering ClearSpending data (comma-separated): ").split(',')
    logging.info(f"Mode set to {'Test' if TEST_MODE else 'Full'} with keywords: {keywords} and exclusions: {excluded_keywords}")
    return keywords, excluded_keywords, product_codes, None


def query_clearspending(inn, api_keys, page_size=50, start_date=DATE_RANGE[0], end_date=DATE_RANGE[1]):
    """Query ClearSpending API for contracts based on INN."""
    url = "https://newapi.clearspending.ru/csinternalapi/v1/filtered-contracts/"
    params = {
        'page_size': page_size,
        'sign_date_gte': start_date,
        'sign_date_lte': end_date,
        'customer_inn': inn,
    }
    all_contracts = []
    page = 1
    key_index = 0
    delay = 5
    keys_exhausted = False

    while True:
        if keys_exhausted:
            logging.warning("All keys exhausted. Waiting for 30 minutes...")
            time.sleep(30 * 60)  # Wait 30 minutes before retrying
            keys_exhausted = False

        params['apikey'] = api_keys[key_index]
        params['page'] = page

        try:
            time.sleep(delay)  # Incremental delay
            response = requests.get(url, params=params)

            if response.status_code == 429:
                logging.warning(f"Rate limit reached. Rotating keys and increasing delay to {delay + 1} seconds...")
                delay += 1
                key_index = (key_index + 1) % len(api_keys)
                if key_index == 0:  # All keys exhausted
                    keys_exhausted = True
                continue

            response.raise_for_status()
            data = response.json()
            all_contracts.extend(data.get('data', []))
            if not data.get('next_page'):
                break
            page += 1
            if delay > 5:  # Reduce delay slightly on successful requests
                delay -= 1

        except requests.exceptions.RequestException as e:
            logging.error(f"Error querying ClearSpending API: {e}")
            break

    return all_contracts


def fetch_clearspending_data_from_inns(inn_list, output_file, api_keys, product_codes):
    """Fetch supplier data from ClearSpending API for a list of INN codes."""
    suppliers_aggregate = []

    for inn_code in inn_list:
        logging.info(f"Querying ClearSpending for INN: {inn_code}")
        contracts = query_clearspending(inn_code, api_keys)
        if not contracts:
            logging.info(f"No contracts found for INN: {inn_code}")
            continue

        # Filter and process contracts based on OKPD2 codes
        for contract in contracts:
            if any(product.startswith(okpd2.strip()) for product in contract.get('product_codes', []) for okpd2 in product_codes):
                supplier_names = contract.get('supplier_names', [None])
                supplier_name = supplier_names[0] if supplier_names else None  # Safely handle empty lists
                supplier_inns = contract.get('supplier_inns', [None])
                supplier_inn = supplier_inns[0] if supplier_inns else None  # Safely handle empty lists

                contract_data = {
                    'Supplier Name': supplier_name,
                    'Supplier INN': supplier_inn,
                    'Total Contract Value': contract.get('amount_rur', 0),
                    'OKPD2 Codes': ', '.join(contract.get('product_codes', [])),
                    'Customer Company Names': contract.get('customer_name', ''),
                    'Customer Company INNs': inn_code,
                }
                suppliers_aggregate.append(contract_data)

    # Save aggregated supplier data to CSV
    output_df = pd.DataFrame(suppliers_aggregate)
    if not output_df.empty:
        output_df.to_csv(output_file, index=False)
        logging.info(f"Supplier data saved to {output_file}")
    else:
        logging.info("No supplier data found.")


def main():
    opensanctions_key, clearspending_keys = get_api_keys()
    keywords, excluded_keywords, product_codes, manual_inns = setup_mode()

    if manual_inns:
        # Fetch ClearSpending data directly using manually entered INNs
        logging.info("Fetching ClearSpending data using manually entered INNs...")
        output_inn_clearspending_path = os.path.join(OUTPUT_DIR, "INN_Clearspending.csv")
        fetch_clearspending_data_from_inns(manual_inns, output_inn_clearspending_path, clearspending_keys, product_codes)
        logging.info(f"Results saved to {output_inn_clearspending_path}")
    else:
        # Run OpenSanctions queries and proceed as usual
        logging.info("Fetching data from OpenSanctions for 'sanction' topic...")
        sanction_results = fetch_opensanctions_data(opensanctions_key, keywords, excluded_keywords, topic='sanction')

        logging.info("Fetching data from OpenSanctions for 'sanction.linked' topic...")
        sanction_linked_results = fetch_opensanctions_data(opensanctions_key, keywords, excluded_keywords, topic='sanction.linked')

        # Combine and deduplicate results
        if sanction_results is not None or sanction_linked_results is not None:
            combined_results = pd.concat([sanction_results, sanction_linked_results])
            deduplicated_results = combined_results.drop_duplicates(subset=['innCode'])

            # Save deduplicated sanctioned entity data
            output_sanction_path = os.path.join(OUTPUT_DIR, "sanctioned_entities.csv")
            deduplicated_results.to_csv(output_sanction_path, index=False)
            logging.info(f"Deduplicated sanctioned data saved to {output_sanction_path}")

            # Fetch ClearSpending data
            output_clearspending_path = os.path.join(OUTPUT_DIR, "clearspending_results.csv")
            fetch_clearspending_data(output_sanction_path, output_clearspending_path, clearspending_keys, product_codes)
        else:
            logging.error("No data found from OpenSanctions. Exiting program.")


if __name__ == "__main__":
    main()
