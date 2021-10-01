import datetime
from datetime import date
import logging
import requests
from requests.exceptions import HTTPError
import pandas as pd
import json

import azure.functions as func

from azure.storage.blob import ContainerClient
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

# Define indicator to get data and API endpoint. Parameter for first year of data (Dim1) is 'S7A2011'
indicator = '0008074'
reqUrl = f"https://www.ine.pt/ine/json_indicador/pindica.jsp?varcd={indicator}&lang=EN&op=2&Dim1="


# Initialize Credentials
default_credential = DefaultAzureCredential()

# Create a Secret Client
ineapi_key_vault = SecretClient(
    vault_url='https://az-function-ine-api.vault.azure.net/',
    credential=default_credential
)

# Get secret for blob storage conenction string
blob_conn_string = ineapi_key_vault.get_secret(
    name='sandbox-storageaccount-connectionstring'
)

# Connect to the Container Client
container_client = ContainerClient.from_connection_string(
    conn_str=blob_conn_string.value,
    container_name='ineapi-blob'
)

# Get date in integet format to append to files on loading to container
today = int(date.today().strftime('%Y%m%d'))


def get_parameters_range(reqUrl: str) -> list:
    '''
    Get parameters to query the API, starting from 2011 until last year available of data. 
    This returns a list of parameters with available years to get data in the API
    '''
    try:
        first_year_parameter = 'S7A2011'
        response = requests.get(reqUrl+first_year_parameter)

        if response.status_code == 200:
            logging.info('Acessing the API to get the data range')
            first_year = 2011
            last_year = int(response.json()[0]['UltimoPref'])
            logging.info(f'Last year of data is {last_year}')
            data_range = list(range(first_year, last_year+1))
            logging.info(
                f'Data from {first_year} until {last_year} will be requested to the API')
            parameters_list = ['S7A' + str(item) for item in data_range]

    except HTTPError as http_err:
        logging.info(f'HTTP error occurred: {http_err}')
    except Exception as err:
        logging.info(f'Other error occurred: {err}')
    else:
        logging.info('Parameters extracted with success!')

    return parameters_list


def get_raw_data(reqUrl: str, parameters_list: list) -> dict:
    '''
    Query the API for each element (year) in parameters_list, starting from 2011 until last year available.
    Returns a dictionary with data for all available years.
    '''
    data = {}
    for item in parameters_list:
        try:
            response = requests.get(reqUrl+item)

            if response.status_code == 200:
                year = item[-4:]
                logging.info(
                    f'Acessing the API to get the data for year {year} ')
                result = response.json()[0]['Dados']
                data.update(result)

        except HTTPError as http_err:
            logging.info(f'HTTP error occurred: {http_err}')
        except Exception as err:
            logging.info(f'Other error occurred: {err}')
        else:
            logging.info(f'Data for year {year} extracted with success!')

    return data


def load_raw_data(raw_data: dict) -> None:
    '''
    Load raw data in blob storage defined, directory '/extract'
    '''

    output_file = f'extract/extract-{today}.json'

    container_client.upload_blob(
        name=output_file,
        data=json.dumps(obj=raw_data, indent=4),
        blob_type='BlockBlob',
        overwrite=True
    )
    logging.info(
        f'Extraction loaded in extract folder. The output file is {output_file}')

    return None


def transform_raw_data(raw_data: dict) -> pd.DataFrame:
    """
    Transform raw data, flattening the nested json in a dataframe, removing unnecessary columns and adding new ones.
    """

    clean_data = []

    for item in raw_data.items():
        col = item[0]
        logging.info(f'Transforming data for year: {col}')
        df_flatenned = pd.json_normalize(raw_data, record_path=col)
        df_flatenned['Year'] = col
        clean_data.append(df_flatenned)

    df = pd.concat(clean_data, ignore_index=True)
    df['Indicator Code'] = indicator
    df['Formule'] = '(Number of crimes/ Resident population)*1000'
    df['Measure Of Unit'] = 'Permillage'
    df.drop(columns=['sinal_conv', 'sinal_conv_desc'], inplace=True)
    df.rename(columns={'geocod': 'Geo Code', 'geodsg': 'Geo',
              'dim_3': 'Crime Code', 'dim_3_t': 'Crime', 'valor': 'Value'}, inplace=True)

    return df


def load_clean_data(clean_data: pd.DataFrame) -> None:
    """
    Load clean data, in a dataframe format, in the blob storage, '/data' directory.
    This will store the data as a csv file.
    """
    output_file = f'data/data-{today}.csv'
    container_client.upload_blob(
        name=output_file,
        data=clean_data.to_csv(index=False),
        blob_type='BlockBlob',
        overwrite=True
    )

    logging.info(
        f'Clean data loaded in data folder. The output file is {output_file}')

    return None


def main(mytimer: func.TimerRequest) -> None:
    """
    Main function that will call the others.
    """
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    logging.info('Starting execution...')

    parameters = get_parameters_range(reqUrl)

    raw_data = get_raw_data(reqUrl, parameters)
    load_raw_data(raw_data)

    clean_data = transform_raw_data(raw_data)
    load_clean_data(clean_data)

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)

    return None
