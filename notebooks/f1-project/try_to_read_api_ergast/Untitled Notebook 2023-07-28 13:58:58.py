# Databricks notebook source
import datetime
import json
import requests

# Define the target folder path in ADLS
target_folder = "/mnt/f1datalakelearn/f1-api-read/drivers/"

# Get the current year
today = datetime.date.today()
year = today.year

print(year)

# Function to check if the file already exists in ADLS
def file_exists_in_adls(year):
    file_path = f"{target_folder}driver_{year}.json"
    return dbutils.fs.ls(file_path)

# Function to fetch F1 driver data for a given year and save it to ADLS
def fetch_and_save_driver_data(year):
    print(year)
    api_url = f"https://ergast.com/api/f1/{year}/drivers.json"
    
    # Fetch data from the API
    response = requests.get(api_url)
    data = response.json()

    # Define the file path for the year
    file_path = f"{target_folder}driver_{year}.json"

    # Create the directory if it doesn't exist
    if not dbutils.fs.ls(target_folder):
        dbutils.fs.mkdirs(target_folder)

    # Check if the file already exists in ADLS, if yes, skip writing
    if not file_exists_in_adls(year):
        # Convert data to JSON strings and join them with newline
        drivers_json = "\n".join(json.dumps(driver) for driver in data['MRData']['DriverTable']['Drivers'])

        # Save the driver data to ADLS
        dbutils.fs.put(file_path, drivers_json, overwrite=True)

        print(f'Writing json for driver {year}')
    else:
        print(f"Skipping year {year} as file already exists in ADLS.")

# Loop through the years and fetch/save the data
for y in range(2020, year + 1):
    fetch_and_save_driver_data(y)


# COMMAND ----------

