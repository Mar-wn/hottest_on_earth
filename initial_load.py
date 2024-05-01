## extract location_dim data

import pycountry
from geopy.geocoders import Nominatim
from google.cloud import bigquery
import pandas as pd


locations_data = {
    'location_PK': [],
    'latitude': [],
    'longitude': []
}

geolocator = Nominatim(user_agent= "agent")

for country in pycountry.countries:

    location = geolocator.geocode(country.name)

    if location:

        print(f'Adding {country.name}')

        locations_data['location_PK'].append(country.name)

        locations_data['latitude'].append(location.latitude)

        locations_data['longitude'].append(location.longitude)
    

## extract time_dim data: hours until 04/2025

from datetime import datetime, timedelta

time_data = {
    'time_PK': [],
    'time': [],
    'year': [],
    'month': [],
    'day': [],
    'hour': []
}

start_date = datetime(2024, 4, 24)

end_date = datetime(2025, 4, 24)

current_date_time = start_date

while current_date_time <= end_date:

    print(f'Adding {current_date_time.strftime("%Y-%m-%d-%H")}')

    time_data['time_PK'].append(int(current_date_time.strftime("%Y%m%d%H")))
    time_data['time'].append(current_date_time)
    time_data['year'].append(current_date_time.year)
    time_data['month'].append(current_date_time.month)
    time_data['day'].append(current_date_time.day)
    time_data['hour'].append(current_date_time.hour)

    current_date_time += timedelta(hours= 1)


## load time_dim and location_dim tables

def load_bigq_table(client, dataset, table, data):

    df = pd.DataFrame(data)

    # write the DataFrame to a temporary CSV file
    csv_file_path = 'temp.csv'

    df.to_csv(csv_file_path, index= False)

    # load the table

    table_ref = client.dataset(dataset).table(table)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.skip_leading_rows = 1  # Skip header row
    job_config.autodetect = True  # Automatically detect schema

    with open(csv_file_path, 'rb') as source_file:

        job = client.load_table_from_file(source_file, table_ref, job_config= job_config)

    # Wait for the job to complete
    job.result()

    print(f'Loaded {job.output_rows} rows into {dataset}.{table}')


project_id = 'hottest-on-earth'

# initialize BigQuery client
client = bigquery.Client(project= project_id)

# load data
load_bigq_table(client, 'daily_temperatures', 'location_dim', locations_data)

load_bigq_table(client, 'daily_temperatures', 'time_dim', time_data)

