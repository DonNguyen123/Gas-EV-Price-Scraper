import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
from datetime import datetime
import time
import re
import os

# 1. Configuration
GITHUB_TOKEN = os.environ.get('GITHUB_TOKEN')  # Changed to use environment variable
GIST_ID = 'e003b91ea818923bcc97dc33b711a0e1'
TARGET_URLS = {
    'gas_prices': 'https://gasprices.aaa.com/todays-state-averages/',
    'ev_prices': 'https://gasprices.aaa.com/ev-charging-prices/'
}

# Headers to simulate a browser request
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept-Language': 'en-US,en;q=0.9',
}

def make_respectful_request(url):
    """Makes a GET request with headers and respects robots.txt delay."""
    print(f"Requesting: {url}")
    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()  # Check for HTTP errors
    print(f"  Status: {response.status_code}")
    return response

def update_github_gist(dataframe, token, gist_id):
    """Updates an existing GitHub Gist with the new data."""
    print(f"\n--- Updating GitHub Gist ---")
    
    if not token:
        print("Error: GITHUB_TOKEN environment variable not set!")
        return None

def extract_table_data(response_text):
    """Extracts table data directly from HTML response text."""
    # Use pandas read_html directly on the response text
    try:
        # Try to read all tables from the HTML
        tables = pd.read_html(response_text)
        if len(tables) == 0:
            raise ValueError("No tables found in the HTML.")
        
        # For AAA website, we want the first table
        df = tables[0]
        
        # Clean column names
        df.columns = [str(col).strip() for col in df.columns]
        
        # Clean state names if 'State' column exists
        if 'State' in df.columns:
            df['State'] = df['State'].astype(str).str.strip()
        elif len(df.columns) > 0:
            # If first column contains states but has different name
            df = df.rename(columns={df.columns[0]: 'State'})
            df['State'] = df['State'].astype(str).str.strip()
        
        return df
    except Exception as e:
        print(f"Error reading table with pandas: {e}")
        # Fallback: Parse manually with BeautifulSoup
        return extract_table_manually(response_text)

def extract_table_manually(html_text):
    """Manual table extraction as fallback when pandas fails."""
    print("Using manual table extraction...")
    soup = BeautifulSoup(html_text, 'html.parser')
    table = soup.find('table')
    
    if not table:
        raise ValueError("No table found in HTML")
    
    # Extract headers
    headers = []
    for th in table.find_all('th'):
        headers.append(th.get_text().strip())
    
    # Extract rows
    rows = []
    for tr in table.find_all('tr')[1:]:  # Skip header row
        cells = tr.find_all(['td', 'th'])
        if cells:
            row_data = [cell.get_text().strip() for cell in cells]
            rows.append(row_data)
    
    # Create DataFrame
    df = pd.DataFrame(rows, columns=headers)
    
    # Clean numeric columns
    for col in df.columns:
        if col != 'State':
            # Remove $ signs and convert to float
            df[col] = df[col].str.replace('$', '', regex=False).astype(float)
    
    return df

def scrape_aaa_gas_prices(url):
    """Scrapes the fuel price table from the first AAA page."""
    print(f"\n--- Scraping Gas Prices ---")
    
    response = make_respectful_request(url)
    
    # Use the direct HTML response for pandas
    df = extract_table_data(response.text)
    
    print(f"Found table with columns: {list(df.columns)}")
    print(f"First few rows:\n{df.head(3)}")
    return df

def scrape_aaa_ev_prices(url):
    """Scrapes the EV charging price table from the second AAA page."""
    print(f"\n--- Scraping EV Prices ---")
    
    # RESPECT THE CRAWL DELAY: Wait 10+ seconds before the second request
    print("Respecting 'Crawl-delay: 10' from robots.txt...")
    for i in range(10, 0, -1):
        print(f"  Waiting... {i} seconds remaining.", end='\r')
        time.sleep(1)
    print("  Proceeding with request.               ")
    
    response = make_respectful_request(url)
    
    # Use the direct HTML response for pandas
    df = extract_table_data(response.text)
    
    # Clean the EV data - ensure proper column names
    if 'Cost/kWh' in df.columns:
        df['Cost/kWh'] = pd.to_numeric(df['Cost/kWh'], errors='coerce')
    
    print(f"Found table with columns: {list(df.columns)}")
    print(f"First few rows:\n{df.head(3)}")
    return df

def merge_data(gas_df, ev_df):
    """Merges the gas price and EV price DataFrames on State."""
    # Ensure consistent State column formatting
    gas_df['State'] = gas_df['State'].str.strip()
    ev_df['State'] = ev_df['State'].str.strip()
    
    # Merge the two DataFrames
    merged_df = pd.merge(gas_df, ev_df, on='State', how='left')
    return merged_df

def update_github_gist(dataframe, token, gist_id):
    """Updates an existing GitHub Gist with the new data."""
    print(f"\n--- Updating GitHub Gist ---")
    
    # Convert DataFrame to a Python dictionary (list of records)
    data_records = dataframe.to_dict(orient='records')
    
    # Create the final payload structure
    full_data = {
        'scrape_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'data': data_records  # This is now a list of dictionaries, not a JSON string
    }
    
    # Prepare the API request
    api_url = f'https://api.github.com/gists/{gist_id}'
    headers = {
        'Authorization': f'token {token}',
        'Accept': 'application/vnd.github+json',
    }
    payload = {
        'description': f'AAA Fuel & EV Price Data - Updated {full_data["scrape_date"]}',
        'files': {
            'aaa_prices.json': {
                'content': json.dumps(full_data, indent=2)  # Single JSON dump here
            }
        }
    }
    
    # Make the PATCH request
    try:
        response = requests.patch(api_url, headers=headers, json=payload, timeout=30)
        
        if response.status_code == 200:
            result = response.json()
            print("Success! Gist updated.")
            print(f"Gist URL: {result['html_url']}")
            
            # Extract the correct raw URL from the API response
            raw_url = result['files']['aaa_prices.json']['raw_url']
            print(f"Raw JSON URL (for your website): {raw_url}")
            return raw_url
        else:
            print(f"Error from GitHub API: {response.status_code}")
            print(f"Response: {response.text[:200]}...")
            return None
            
    except requests.exceptions.Timeout:
        print("Error: Request to GitHub API timed out.")
        return None
    except requests.exceptions.RequestException as e:
        print(f"Network error: {e}")
        return None

def main():
    """Main function to run the entire scraping and upload process."""
    try:
        print("="*60)
        print("Starting AAA Daily Data Scraper")
        print("Respecting robots.txt (Crawl-delay: 10 seconds)")
        print("="*60)
        
        # Scrape data from both websites
        gas_df = scrape_aaa_gas_prices(TARGET_URLS['gas_prices'])
        ev_df = scrape_aaa_ev_prices(TARGET_URLS['ev_prices'])
        
        # Display summary
        print(f"\n--- Summary ---")
        print(f"Gas prices: {len(gas_df)} states/regions")
        print(f"EV prices: {len(ev_df)} states/regions")
        
        # Merge the data
        merged_df = merge_data(gas_df, ev_df)
        print(f"Merged data: {len(merged_df)} total entries")
        print(f"\nSample of merged data (first 3 rows):")
        print(merged_df.head(3).to_string())
        
        # Update the GitHub Gist
        raw_url = update_github_gist(merged_df, GITHUB_TOKEN, GIST_ID)
        
        if raw_url:
            print("\n" + "="*60)
            print("SCRAPING PROCESS COMPLETE!")
            print("="*60)
            print(f"\nFor your website, use this URL to fetch data:")
            print(f"{raw_url}")
            print("\nJavaScript fetch example:")
            print(f"""fetch('{raw_url}')
  .then(response => response.json())
  .then(data => {{
    console.log('Last updated:', data.scrape_date);
    console.log('Number of states:', data.data.length);
    // Access state data: data.data
    data.data.forEach(state => {{
      console.log(state.State, state.Regular, state['Cost/kWh']);
    }});
  }});""")
        else:
            print("\nFailed to update Gist. Check the error above.")
        
    except requests.exceptions.HTTPError as e:
        print(f"\nHTTP Error: {e}")
        if e.response.status_code == 403:
            print("The website is blocking the request.")
            print("Try increasing the crawl delay or using different headers.")
    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()