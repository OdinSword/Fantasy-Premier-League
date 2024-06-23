import re
from unidecode import unidecode
import requests
from requests.exceptions import RequestException
import pandas as pd
import yaml
from src.constants import yaml_data


class extract_data:
     
    # init method or constructor
    def __init__(self, config: dict) -> None:
        self.config = config



    ##downloading data from the endpoint
    def download(self, endpoint: str) -> dict:
      try:
          response = requests.get(endpoint)
          response.raise_for_status()  # Raise an HTTPError for bad responses (4xx and 5xx)
          return response.json()
      except requests.exceptions.HTTPError as http_err:
          if response.status_code == 404:
              print("Error: 404 received")
              return {"error": "404"}
          else:
              print(f"HTTP error occurred: {http_err}")
              return {"error": f"HTTP error {response.status_code}"}
      except RequestException as err:
          print(f"Error occurred: {err}")
          return {"error": str(err)}


      
    ##converting json data into pandas dataframe      
    def to_df_dict(self, json: dict, keys: list) -> dict:
        df_dict = {}
  
        for key in keys:
          df_dict[key] = pd.DataFrame(json.get(key))
        
        return df_dict


    ##Extracting data from 
    def extract(self) -> dict:
        d2 = {}
        table2 = []

        config_api = self.config['api']
        print(config_api)
        
        if 'endpoints' in config_api:
          url = ''
          for endpoint in config_api['endpoints']:
            
            if 'tables' in endpoint:
              url = config_api['baseurl'] + '/' + endpoint['name'] + '/' 
              
              d2.update(self.to_df_dict(self.download(url), endpoint['tables']))

            else:
              url = config_api['baseurl'] + '/' + endpoint['name'] + '/' 
              r1 = self.download(url)
              x1 = {endpoint['name']: r1}
              table2.append(endpoint['name'])
              d2.update(self.to_df_dict(x1, table2)) 


          return d2


    def run(self) -> None:
        df_dict_raw = self.extract()


        return df_dict_raw
