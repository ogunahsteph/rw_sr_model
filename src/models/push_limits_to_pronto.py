import requests
import pendulum
import datetime
local_tz = pendulum.timezone("Africa/Nairobi")

import os
from dotenv import load_dotenv
load_dotenv()

name = os.environ.get('pronto_name')
phrase = os.environ.get('pronto_phrase')

response = requests.post(
   url = 'https://airflow.asantefsg.com/data-pipelines/api/v1/dags/ETL_pronto_scoring_results/dagRuns',
   headers = {'Content-type': 'application/json', 'Accept': 'application/json'},
   json = {
      "execution_date": str(datetime.datetime.now().replace(tzinfo = local_tz)),
      "conf": {
         'tasks_to_run': [
            'load_bralirwa_scoring_results',
            # 'load_obradleys_scoring_results',
            # 'load_hugeshare_scoring_results',
            # 'load_rwanda_ac_group_scoring_results',
            # 'load_copia_device_financing_scoring_results',
            # 'load_kenya_airways_scoring_results'
         ]}
   },
   auth = requests.auth.HTTPBasicAuth(name, phrase),
   verify = False
)
if response.status_code != 200:
    print(response.status_code, response.text)
else:
    print('Pipeline triggered successfully.')