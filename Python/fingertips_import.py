import requests
import pandas as pd
import io
import logging
from tqdm import tqdm

# Set up logging
logging.basicConfig(
    filename='ftp_import_log.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def get_fingertips_indicators(indicator_ids, area_codes=None):
    """
    Downloads indicator data from the Fingertips API for one or more indicator IDs.
    Shows a progress bar for each indicator download.
    Filters results to only include specified area codes (if provided).
    Returns a combined pandas DataFrame of all successful downloads.
    """
    base_url = "https://fingertips.phe.org.uk/api/all_data/csv/by_indicator_id"
    all_dataframes = []

    # Ensure indicator_ids is a list of strings
    if isinstance(indicator_ids, (int, str)):
        indicator_ids = [indicator_ids]
    indicator_ids = [str(i) for i in indicator_ids]

    # Ensure area_codes is a list if provided
    if area_codes is not None and isinstance(area_codes, str):
        area_codes = [area_codes]

    # Progress bar for downloading each indicator
    for indicator_id in tqdm(indicator_ids, desc="Downloading indicators"):
        url = f"{base_url}?indicator_ids={indicator_id}"
        try:
            response = requests.get(url)
            response.raise_for_status()
            text = response.text
            if not text.strip() or text.strip().startswith("No data"):
                logging.warning(f"No data returned for indicator ID {indicator_id}")
                continue
            df = pd.read_csv(io.StringIO(text), dtype=str, low_memory=False)
            if df.empty:
                logging.warning(f"Empty DataFrame for indicator ID {indicator_id}")
                continue
            all_dataframes.append(df)
            logging.info(f"Successfully retrieved indicator {indicator_id}")
        except Exception as e:
            logging.error(f"Error retrieving indicator {indicator_id}: {e}")
            continue

    if not all_dataframes:
        return pd.DataFrame()

    # Combine all dataframes
    combined_df = pd.concat(all_dataframes, ignore_index=True)

    # Filter by area_codes if provided
    if area_codes is not None:
        # Look for any column that matches 'areacode' ignoring case and spaces
        area_code_col = None
        for col in combined_df.columns:
            if col.replace(" ", "").lower() == "areacode":
                area_code_col = col
                break
        if area_code_col:
            combined_df = combined_df[combined_df[area_code_col].isin(area_codes)]
    return combined_df

def list_area_types():
    """
    Fetches and returns a DataFrame of available area types from the Fingertips API.
    Prints errors to console if the request fails.
    """
    url = "https://fingertips.phe.org.uk/api/area_types"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        df = pd.DataFrame(data)
        print(f"✅ Retrieved {len(df)} area types.")
        return df
    except Exception as e:
        print(f"❌ Error retrieving area types: {e}")
        return pd.DataFrame()  # Return empty DataFrame on failure
