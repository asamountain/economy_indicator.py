import requests
import pandas as pd
import json
from datetime import datetime


def fetch_apis(api_json):

    # Open and load the JSON file
    with open(api_json, "r", encoding="utf-8") as file:
        fetched_api_json = json.load(file)  # Convert JSON to Python dictionary

    # Test
    print("Fetched APIs : ", fetched_api_json)
    return fetched_api_json


def fetch_jolts_data(fetched_api_json):
    # Extract API key from config (ensure your config.json has the expected structure)
 
    jolts_api_key = fetched_api_json.get("BLS_JOLRT", {}).get("BLS_PUBLIC_DATA_API")

    if not jolts_api_key:
        print("Error: API key not found in config.json")
        return None

    # Compute startyear and endyear to cover at least the last 12 months.
    # (This assumes that data for both the previous and current year is available.)
    today = datetime.today()
    start_year = str(today.year - 20)
    end_year = str(today.year)

    # Set the endpoint and headers
    url = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
    headers = {'Content-type': 'application/json'}

    # Build the payload using POST method (modify startyear and endyear as needed)
    payload = {
        "seriesid": ['JTS000000000000000JOR'],
        "startyear": start_year,
        "endyear": end_year,
        "registrationkey": jolts_api_key  # Include registration key if required by the API
    }
    data = json.dumps(payload)
    
    # Send POST request
    response = requests.post(url, data=data, headers=headers)

    # Debug prints to help verify the response
    print("API Response Status Code:", response.status_code)
    print("API Response Text:", response.text[:500])  # Print the first 500 characters

    try:
        json_data = response.json()
    except json.JSONDecodeError:
        print("Error: API response is not valid JSON.")
        return None

    # Check for expected keys in the response
    if ("Results" not in json_data or 
        "series" not in json_data["Results"] or 
        not json_data["Results"]["series"]):
        print("Error: Unexpected API response format:", json_data)
        return None

    # Convert the first series' data into a DataFrame
    series = json_data["Results"]["series"][0]
    df = pd.DataFrame(series["data"])
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["date"] = pd.to_datetime(df["year"] + "-" + df["period"].str[1:] + "-01")
    
    return df[["date", "value"]]

def main():
    api_json = "./config.json"
    fetched_api_json = fetch_apis(api_json)
    jolts_df = fetch_jolts_data(fetched_api_json)
    if jolts_df is not None:
        print("Fetched Jolts Data:")
        print(jolts_df)
    else:
        print("No data fetched.")


if __name__ == "__main__":
    main()
