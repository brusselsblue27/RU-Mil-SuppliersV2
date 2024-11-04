import os
import csv
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
TEST_MODE = False  # Set this to True to limit to 10 companies for testing
API_RETRY_TIMEOUT = 20 * 60  # 20 minutes in seconds

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
    
    # Prompt for keywords to include in the search
    keywords = input("Enter keywords for search (comma-separated): ").split(',')
    
    # Default excluded keywords
    default_exclusions = ["banks", "politics", "medical"]
    user_exclusions = input(f"Enter keywords to exclude (default: {', '.join(default_exclusions)}): ")
    
    # Use provided exclusions or fall back to defaults
    excluded_keywords = user_exclusions.split(',') if user_exclusions else default_exclusions
    
    # Inform the user of the settings
    logging.info(f"Mode set to {'Test' if TEST_MODE else 'Full'} with keywords: {keywords} and exclusions: {excluded_keywords}")
    
    # Prompt for product codes if needed
    product_codes = input("Enter product codes (OKPD2) for filtering ClearSpending data (comma-separated): ").split(',')
    return keywords, excluded_keywords, product_codes

def fetch_opensanctions_data(api_key, keywords, excluded_keywords):
    """Fetch sanctioned entities using OpenSanctions API and save using pandas."""
    logging.debug("Starting fetch_opensanctions_data...")
    url = "https://api.opensanctions.org/search/sanctions"
    batch_size = 100
    all_results = []
    
    def extract_tax_info(properties):
        tax_info = {'taxNumber': None, 'innCode': None}
        tax_info['taxNumber'] = properties.get('taxNumber', [None])[0]
        tax_info['innCode'] = properties.get('innCode', [None])[0]
        return tax_info

    def extract_russian_aliases(properties):
        """Extract unique Russian aliases from entity properties, excluding Ukrainian names."""
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
    
    def calculate_richness_score(row):
        """Assign a richness score based on populated fields."""
        score = 0
        fields = ['innCode', 'taxNumber', 'caption'] + [col for col in row.index if col.startswith('russian_alias_')]
        score += sum(1 for field in fields if pd.notnull(row[field]))
        return score

    for keyword in keywords:
        offset = 0
        more_results = True
        logging.info(f"Searching for keyword: {keyword}")
        
        while more_results:
            query_params = {
                'q': keyword,
                'countries': 'RU',
                'schema': 'LegalEntity',
                'topics': 'sanction',
                'fuzzy': 'true',
                'limit': batch_size,
                'offset': offset
            }
            headers = {'Authorization': f'Bearer {api_key}'}
            
            try:
                logging.debug(f"Sending request to OpenSanctions API with params: {query_params}")
                response = requests.get(url, headers=headers, params=query_params)
                response.raise_for_status()
                data = response.json()
                results = data.get('results', [])
                
                if not results:
                    logging.info(f"No more results found for keyword: {keyword} at offset: {offset}")
                    break
                
                for result in results:
                    schema_type = result.get('schema', '').lower()
                    entity_caption = result.get('caption', '').lower()
                    entity_properties = result.get('properties', {})
                    
                    if schema_type == "person":
                        logging.debug(f"Skipping entity of type 'person': {entity_caption}")
                        continue
                    
                    if any(exclude_kw in entity_caption for exclude_kw in excluded_keywords):
                        logging.debug(f"Excluding entity due to excluded keywords: {entity_caption}")
                        continue
                    
                    tax_info = extract_tax_info(entity_properties)
                    if tax_info['taxNumber'] and is_valid_inn(tax_info['taxNumber']) and not tax_info['innCode']:
                        tax_info['innCode'] = tax_info['taxNumber']
                    
                    result['taxNumber'] = tax_info['taxNumber']
                    result['innCode'] = tax_info['innCode']
                    
                    russian_aliases = extract_russian_aliases(entity_properties)
                    for i, alias in enumerate(russian_aliases):
                        result[f'russian_alias_{i+1}'] = alias
                    
                    all_results.append(result)

                if len(results) < batch_size:
                    more_results = False
                else:
                    offset += batch_size
            except requests.exceptions.RequestException as e:
                logging.error(f"Failed to fetch data from OpenSanctions API: {str(e)}")
                more_results = False

    # Handle duplicates by INN and select the row with the highest richness score
    df = pd.DataFrame(all_results)
    if not df.empty:
        df['richness_score'] = df.apply(calculate_richness_score, axis=1)
        df = df.sort_values('richness_score', ascending=False).drop_duplicates('innCode', keep='first')
    
    # Prompt the user for missing INNs if needed
    missing_inns = df[df['innCode'].isna()]
    for idx, row in missing_inns.iterrows():
        company_name = row['caption']
        print(f"Missing INN for {company_name}. Please search for it and enter it here (or press Enter to skip):")
        user_inn = input("INN: ").strip()
        if user_inn and is_valid_inn(user_inn):
            df.at[idx, 'innCode'] = user_inn
        else:
            logging.info(f"No INN entered for {company_name}, skipping this entry.")

    # Save the finalized DataFrame
    output_path = os.path.join(OUTPUT_DIR, "sanctioned_entities.csv")
    df_selected_columns = ['id', 'caption', 'schema', 'taxNumber', 'innCode'] + \
                          [col for col in df.columns if col.startswith('russian_alias_')]
    df[df.columns.intersection(df_selected_columns)].to_csv(output_path, index=False)
    logging.info(f"Data saved to {output_path}")
    return output_path

def fetch_clearspending_data(input_file, output_file, api_keys, product_codes):
    """Fetch supplier data from ClearSpending API for each INN code with rate-limiting and timeout."""
    base_url = "https://newapi.clearspending.ru/csinternalapi/v1/filtered-contracts/"
    current_key_index = 0

    def switch_api_key():
        nonlocal current_key_index
        current_key_index += 1
        if current_key_index >= len(api_keys):
            logging.error("All API keys have reached their quota. Please try again later.")
            raise SystemExit
        logging.info(f"Switching to API Key #{current_key_index + 1}")

    def query_clearspending(inn, page_size=50, start_date=DATE_RANGE[0], end_date=DATE_RANGE[1], product_codes=None):
        params = {
            'apikey': api_keys[current_key_index],
            'page_size': page_size,
            'sort': '-amount_rur',
            'sign_date_gte': start_date,
            'sign_date_lte': end_date,
            'customer_inn': inn,
            'product_codes': ','.join(product_codes) if product_codes else None
        }
        try:
            logging.debug(f"Sending request to ClearSpending API with params: {params}")
            response = requests.get(base_url, params=params)
            if response.status_code == 429:
                logging.warning(f"Rate limit exceeded for API Key #{current_key_index + 1}")
                switch_api_key()
                time.sleep(5)
                return query_clearspending(inn, page_size, start_date, end_date, product_codes)
            if response.status_code == 200:
                return response.json()
            else:
                logging.error(f"INN search failed: {response.status_code} - {response.text}")
        except requests.exceptions.RequestException as e:
            logging.error(f"Error querying the API: {e}")
        return None

    logging.debug(f"Reading input file: {input_file}")
    data = pd.read_csv(input_file)
    output_data = []
    for index, row in data.iterrows():
        if TEST_MODE and index >= 10:
            break
        caption = row['caption']
        inn_code = str(int(row['innCode'])) if not pd.isnull(row['innCode']) else None
        if inn_code:
            result = query_clearspending(inn=inn_code, product_codes=product_codes)
            if result:
                contracts = result.get('data', [])
                suppliers = {}
                for contract in contracts:
                    for supplier_inn, supplier_name, amount in zip(
                        contract.get('supplier_inns', []),
                        contract.get('supplier_names', []),
                        [contract.get('amount_rur')] * len(contract.get('supplier_inns', []))
                    ):
                        if supplier_inn not in suppliers:
                            suppliers[supplier_inn] = {'supplier_name': supplier_name, 'total_value': amount}
                        else:
                            suppliers[supplier_inn]['total_value'] += amount

                top_suppliers = sorted(suppliers.items(), key=lambda x: x[1]['total_value'], reverse=True)[:3]
                supplier_data = {
                    'Company Name': caption,
                    'Company INN': inn_code,
                    'Supplier 1 Name': top_suppliers[0][1]['supplier_name'] if len(top_suppliers) > 0 else None,
                    'Supplier 1 INN': top_suppliers[0][0] if len(top_suppliers) > 0 else None,
                    'Supplier 1 Contract Value': top_suppliers[0][1]['total_value'] if len(top_suppliers) > 0 else None,
                    'Supplier 2 Name': top_suppliers[1][1]['supplier_name'] if len(top_suppliers) > 1 else None,
                    'Supplier 2 INN': top_suppliers[1][0] if len(top_suppliers) > 1 else None,
                    'Supplier 2 Contract Value': top_suppliers[1][1]['total_value'] if len(top_suppliers) > 1 else None,
                    'Supplier 3 Name': top_suppliers[2][1]['supplier_name'] if len(top_suppliers) > 2 else None,
                    'Supplier 3 INN': top_suppliers[2][0] if len(top_suppliers) > 2 else None,
                    'Supplier 3 Contract Value': top_suppliers[2][1]['total_value'] if len(top_suppliers) > 2 else None
                }
                if any(supplier_data.values()):
                    output_data.append(supplier_data)
            else:
                logging.info(f"No results found for company: {caption}")
            logging.debug(f"Company INN: {inn_code}")
        time.sleep(5)

    output_df = pd.DataFrame(output_data)
    logging.debug(f"Writing output data to file: {output_file}")
    output_df.to_csv(output_file, index=False)
    logging.info(f"Data saved to {output_file}")
    return output_file

def main():
    opensanctions_key, clearspending_keys = get_api_keys()
    keywords, excluded_keywords, product_codes = setup_mode()
    
    # Phase 1: Fetch data from OpenSanctions
    logging.info("Fetching data from OpenSanctions...")
    inn_csv_path = fetch_opensanctions_data(opensanctions_key, keywords, excluded_keywords)
    if inn_csv_path:
        logging.info(f"Data saved to {inn_csv_path}")

        # Phase 2: Fetch supplier data from ClearSpending
        output_path = os.path.join(OUTPUT_DIR, "clearspending_results.csv")
        logging.debug("Starting fetch_clearspending_data...")
        fetch_clearspending_data(inn_csv_path, output_path, clearspending_keys, product_codes)
    else:
        logging.error("Failed to fetch OpenSanctions data. Exiting program.")

if __name__ == "__main__":
    main()
