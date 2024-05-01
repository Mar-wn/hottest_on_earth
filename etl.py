from datetime import datetime

current_time = datetime.now()
print(f"\n{100 * '#'} \n\nExtracting countries data on {current_time}")

import json
import pycountry
import pandas as pd
from google.cloud import bigquery
import requests


data = {
    'location_FK': [],
    'time_FK': [],
    'load_time': [],
    'avg_temperature': []
}

headers = {"accept": "application/json"}

countries_names = [country.name for country in pycountry.countries]

for country_name in countries_names[:5]: 

    url = f"https://api.tomorrow.io/v4/weather/history/recent?location={country_name}&timesteps=1d&apikey=e9sODPo4ogRW3Mr2BW0Oh5G8x4mLMoRZ"
   
    response = requests.get(url, headers= headers)

    if response.status_code == 200:

        print(country_name, ' Done')

        response_dict = json.loads(response.text)

        time = datetime.strptime(response_dict['timelines']['daily'][0]['time'], "%Y-%m-%dT%H:%M:%SZ")
        
        avg_temp = response_dict['timelines']['daily'][0]['values']['temperatureAvg']

        data['location_FK'].append(country_name)
        data['time_FK'].append(int(time.strftime("%Y%m%d%H")))
        data['load_time'].append(current_time)
        data['avg_temperature'].append(avg_temp)

    elif response.status_code == 429:

        print('Limit Issue')

    else:

        print(country_name, ' Issue')

project_id = 'hottest-on-earth'
dataset_id = 'daily_temperatures'
table_id = 'temperatures_fact'

# initialize BigQuery client
client = bigquery.Client(project= project_id)

# convert the dictionary to a DataFrame
df = pd.DataFrame(data)

# construct the reference to the table
table_ref = client.dataset(dataset_id).table(table_id)

# write the DataFrame to a temporary CSV file
csv_file_path = '/home/marwen/Applications/experiments/hottest_on_earth/temp.csv' 
df.to_csv(csv_file_path, index= False)

# load the data from the CSV file into the BigQuery table
job_config = bigquery.LoadJobConfig()
job_config.source_format = bigquery.SourceFormat.CSV
job_config.skip_leading_rows = 1  # Skip header row
job_config.autodetect = True  # Automatically detect schema

with open(csv_file_path, 'rb') as source_file:

    job = client.load_table_from_file(source_file, table_ref, job_config= job_config)

# wait for the job to complete
job.result()

print(f'Loaded {job.output_rows} rows into {dataset_id}.{table_id}')
