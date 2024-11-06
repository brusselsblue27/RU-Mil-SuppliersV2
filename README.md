# OpenSanctions and ClearSpending Data Extraction Tool

This project is a Python tool that retrieves and processes data from the OpenSanctions and ClearSpending APIs. It’s designed to filter and enrich company data with a focus on sanctions, aliases, INN (Tax ID) information, and supplier contracts. The tool includes features like deduplication by INN, data completeness scoring, and OKPD2-based filtering.

## Features

- **Data Retrieval from OpenSanctions and ClearSpending**: Collects and enriches company data using two APIs, with customizable search keywords, exclusions, and product codes.
- **Data Deduplication with Richness Score**: Identifies duplicate companies by INN and retains the version with the most comprehensive data.
- **Manual INN Entry**: Prompts for manual entry of missing INNs to enhance data completeness.
- **OKPD2 Code Filtering**: Filters ClearSpending contracts based on specified OKPD2 product codes.
- **Environment Variable Management**: Loads sensitive information from a `.env` file to protect API keys.

## Prerequisites

- Python 3.6 or higher
- The required Python packages listed in `requirements.txt`

## Setup

1. **Clone the repository**:
   ```bash
   git clone https://github.com/brusselsblue27/RU-Mil-SuppliersV2
   cd Ru-Mil4

2. Insert API keys into .env file.
