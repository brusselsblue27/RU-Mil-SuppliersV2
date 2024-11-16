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
    
    keywords = input("Enter keywords for search (comma-separated): ").split(',')
    default_exclusions = ["banks", "politics", "medical"]
    user_exclusions = input(f"Enter keywords to exclude (default: {', '.join(default_exclusions)}): ")
    excluded_keywords = user_exclusions.split(',') if user_exclusions else default_exclusions
    
    product_codes = input("Enter product codes (OKPD2) for filtering ClearSpending data (comma-separated): ").split(',')
    logging.info(f"Mode set to {'Test' if TEST_MODE else 'Full'} with keywords: {keywords} and exclusions: {excluded_keywords}")
    return keywords, excluded_keywords, product_codes

def extract_russian_aliases(properties):
    """Extract unique Russian aliases from entity properties, excluding Ukrainian and other languages."""
    russian_specific_chars = {'ы', 'э', 'й'}
    ukrainian_specific_chars = {'є', 'і', 'ї', 'ґ'}
    
    aliases = set()
    for field in ['alias', 'name']:
        for alias in properties.get(field, []):
            if isinstance(alias, dict) and alias.get('lang') == 'rus':
                alias_value = alias.get('value', '').lower()
                if any(char in russian_specific_chars for char in alias_value) and not any(char in ukrainian_specific_chars for char in alias_value):
                    aliases.add(alias_value)
            elif isinstance(alias, str):
                alias_value = alias.lower()
                if any(char in russian_specific_chars for char in alias_value) and not any(char in ukrainian_specific_chars for char in alias_value):
                    aliases.add(alias_value)
    return list(aliases)

def fetch_opensanctions_data(api_key, keywords, excluded_keywords, topic):
    """Fetch sanctioned entities using OpenSanctions API for a specific topic."""
    url = "https://api.opensanctions.org/search/sanctions"
    batch_size = 100
    all_results = []

    for keyword in keywords:
        offset = 0
        more_results = True
        while more_results:
            query_params = {
                'q': keyword,
                'countries': 'RU',
                'schema': 'LegalEntity',
                'topics': topic,
                'fuzzy': 'true',
                'limit': batch_size,
                'offset': offset
            }
            headers = {'Authorization': f'Bearer {api_key}'}
            logging.debug(f"Querying OpenSanctions with parameters: {query_params}")

            try:
                response = requests.get(url, headers=headers, params=query_params)
                response.raise_for_status()
                data = response.json()
                results = data.get('results', [])
                
                if not results:
                    logging.info(f"No more results for keyword: {keyword}")
                    break

                for result in results:
                    schema_type = result.get('schema', '').lower()

                    # Skip entities of type "Person"
                    if schema_type == "person":
                        logging.info(f"Skipping individual: {result.get('caption', 'Unknown')}")
                        continue

                    # Exclude entities with specified keywords
                    entity_caption = result.get('caption', '').lower()
                    entity_properties = result.get('properties', {})
                    if any(excluded_kw.lower() in entity_caption for excluded_kw in excluded_keywords):
                        logging.info(f"Excluding entity: {entity_caption} due to excluded keyword.")
                        continue

                    aliases = entity_properties.get('alias', [])
                    if any(excluded_kw.lower() in alias.lower() for alias in aliases for excluded_kw in excluded_keywords):
                        logging.info(f"Excluding entity due to alias match: {entity_caption}")
                        continue

                    inn_code = result.get('properties', {}).get('innCode', [None])[0]
                    tax_number = result.get('properties', {}).get('taxNumber', [None])[0]

                    if not inn_code and tax_number and is_valid_inn(tax_number):
                        inn_code = tax_number

                    if not inn_code:
                        user_input = input(f"Missing INN for {result.get('caption', 'Unknown')}. Enter INN (or press Enter to skip): ").strip()
                        if user_input and is_valid_inn(user_input):
                            inn_code = user_input
                        else:
                            logging.info(f"Skipping entity {result.get('caption', 'Unknown')} due to missing INN.")
                            continue

                    russian_aliases = extract_russian_aliases(result.get('properties', {}))
                    for i, alias in enumerate(russian_aliases):
                        result[f'russian_alias_{i+1}'] = alias

                    result['innCode'] = inn_code
                    all_results.append(result)

                offset += batch_size

            except requests.exceptions.RequestException as e:
                logging.error(f"Error querying OpenSanctions API: {e}")
                more_results = False

    # Deduplicate by INN using richness score
    if all_results:
        df = pd.DataFrame(all_results)
        df['richness'] = df.apply(lambda row: sum(pd.notnull(row[col]) for col in row.index if col.startswith('russian_alias_')), axis=1)
        df = df.sort_values('richness', ascending=False).drop_duplicates('innCode', keep='first')
        df.drop(columns=['richness'], inplace=True)
        logging.info("Deduplication complete after fetching OpenSanctions data.")

        # Save to CSV
        output_path = os.path.join(OUTPUT_DIR, f"sanctioned_entities.csv")
        df.to_csv(output_path, index=False)
        logging.info(f"Data saved to {output_path}")
        return df
    else:
        logging.warning("No results found from OpenSanctions API.")
        return None

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

def fetch_clearspending_data(input_file, output_file, api_keys, product_codes):
    """Fetch supplier data from ClearSpending API for each INN code and filter results based on OKPD2 codes locally."""
    data = pd.read_csv(input_file)

    # Ensure INNs are strings and clean up formatting
    data['innCode'] = data['innCode'].astype(str).str.strip().str.replace(r'\.0$', '', regex=True)

    suppliers_aggregate = {}

    if TEST_MODE:
        data = data.head(20)

    for index, row in data.iterrows():
        inn_code = row['innCode']
        if not is_valid_inn(inn_code):
            user_input = input(f"Invalid INN detected for {row['caption']}. Please confirm or correct the INN (or press Enter to skip): ").strip()
            if user_input and is_valid_inn(user_input):
                inn_code = user_input
            else:
                logging.info(f"Skipping company {row['caption']} due to missing or invalid INN.")
                continue

        logging.info(f"Querying ClearSpending for INN: {inn_code}")
        contracts = query_clearspending(inn_code, api_keys)
        if not contracts:
            logging.info(f"No contracts found for INN: {inn_code}")
            continue

        # Filter and process contracts based on OKPD2 codes
        filtered_contracts = [
            contract for contract in contracts
            if any(
                product.startswith(okpd2.strip()) for product in contract.get('product_codes', []) for okpd2 in product_codes
            )
        ]

        for contract in filtered_contracts:
            supplier_inns = contract.get('supplier_inns', [])
            supplier_names = contract.get('supplier_names', [])
            amount = contract.get('amount_rur', 0)

            for supplier_inn, supplier_name in zip(supplier_inns, supplier_names):
                if supplier_inn not in suppliers_aggregate:
                    suppliers_aggregate[supplier_inn] = {
                        'Supplier Name': supplier_name,
                        'Supplier INN': supplier_inn,
                        'Total Contract Value': amount,
                        'OKPD2 Codes': set(contract.get('product_codes', [])),
                        'Customer Company Names': {row['caption']},
                        'Customer Company INNs': {inn_code}
                    }
                else:
                    suppliers_aggregate[supplier_inn]['Total Contract Value'] += amount
                    suppliers_aggregate[supplier_inn]['OKPD2 Codes'].update(contract.get('product_codes', []))
                    suppliers_aggregate[supplier_inn]['Customer Company Names'].add(row['caption'])
                    suppliers_aggregate[supplier_inn]['Customer Company INNs'].add(inn_code)

    # Consolidate supplier data into a list of dictionaries for output
    output_data = []
    for supplier_info in suppliers_aggregate.values():
        output_data.append({
            'Supplier Name': supplier_info['Supplier Name'],
            'Supplier INN': supplier_info['Supplier INN'],
            'Total Contract Value': supplier_info['Total Contract Value'],
            'OKPD2 Codes': ', '.join(supplier_info['OKPD2 Codes']),
            'Customer Company Names': ', '.join(supplier_info['Customer Company Names']),
            'Customer Company INNs': ', '.join(supplier_info['Customer Company INNs']),
        })

    # Save aggregated supplier data to CSV
    if output_data:
        output_df = pd.DataFrame(output_data)
        output_df.to_csv(output_file, index=False)
        logging.info(f"Supplier data saved to {output_file}")
    else:
        logging.info("No supplier data found.")

def main():
    opensanctions_key, clearspending_keys = get_api_keys()
    keywords, excluded_keywords, product_codes = setup_mode()

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
