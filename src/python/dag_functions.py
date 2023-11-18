import os
import csv
import time
import boto3
import pickle
import logging
import botocore
import requests
import datetime
from typing import Union
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(filename='application.log', level=logging.DEBUG)

# HELPER FUNCTIONS

def parse_csv_to_list(filepath) -> list:
    try:
        with open(filepath, newline="") as file:
            csv_reader = csv.reader(file)
            next(csv_reader, None)
            data_list = [[None if value == "None" else value for value in row] for row in csv_reader]
            
        return data_list
    except FileNotFoundError:
        logging.error(f"Error: File not found at '{filepath}'.")
        return []
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return []

def create_output_folder(folder_type: str) -> str:
    valid_folders = ['raw-data', 'processed-data']
    if folder_type not in valid_folders:
        error_message = f'The folder to create should be named as either {valid_folders[0]} or {valid_folders[1]}.'
        logging.error(error_message)
        raise ValueError(error_message)
    
    current_dir = os.path.dirname(os.path.abspath(__file__))
    data_folder = os.path.join(current_dir, '../..', folder_type)
    os.makedirs(data_folder, exist_ok=True)
    return data_folder

def delete_local_file(ti, **kwargs) -> None:
    for file_info in kwargs.get('files_to_delete', []):
        pulled_task_id = file_info['pulled_task_id']
        pulled_key = file_info['pulled_key']
        file_path = ti.xcom_pull(task_ids=pulled_task_id, key=pulled_key)
        try:
            os.remove(file_path)
            print(f"File '{file_path}' deleted successfully.")
        except OSError as e:
            print(f"Error occurred while deleting file '{file_path}': {e}")

# DATA EXTRACTION FUNCTIONS

def get_financials(tickers: list, function: str, **kwargs) -> str:
    av_key = os.environ.get('AV_KEY')
    if av_key is None:
        error_message = 'AV_KEY Airflow Variable is missing.'
        logging.error(error_message)
        raise ValueError(error_message)    
    
    raw_data = []
    max_retry_attempts = 3
    for ticker in tickers:
        retry_count = 0
        while retry_count < max_retry_attempts:
            starttime = time.perf_counter()
            url = f'https://www.alphavantage.co/query?function={function}&symbol={ticker}&apikey={av_key}'
            try:
                response = requests.get(url)
                response.raise_for_status()
            except requests.exceptions.HTTPError as e:
                logging.error('HTTP Error occurred:', e)
                retry_count += 1
                continue
            except requests.exceptions.RequestException as e:
                logging.error('Error occurred during the request: %s', e)
                retry_count += 1
                continue
            except Exception as e:
                logging.error('An unexpected error occurred:', e)
                retry_count += 1
                continue
            endtime = time.perf_counter()
            data = response.json()
            raw_data.append(data)
            duration = round((endtime - starttime), 2)
            print(f'Getting the data for {ticker} was successful and it took {duration} seconds.')
            try:
                time.sleep(12 - duration)  # 5 calls per minute for Alpha Vantage API - the elapsed time of the request
            except ValueError as v:
                logging.error('The API response took more than a minute; no need for further wait.')
                pass
            break
        retry_count += 1
        if retry_count == max_retry_attempts:
            logging.error(f'Reached maximum retry attempts for {ticker}. Skipping.')

    raw_file = f'raw_data_{function}_{datetime.datetime.now()}.pkl'
    data_folder = create_output_folder('raw-data')
    raw_file_path = os.path.join(data_folder, raw_file)

    with open(raw_file_path, 'wb') as rawfile:
        pickle.dump(raw_data, rawfile)

    print(f'{raw_file} was created successfully at {data_folder}.')
    kwargs['ti'].xcom_push(key='raw_file_path', value=raw_file_path)
    return raw_file_path

# DATA VALIDATION FUNCTIONS

def weekly_metrics_are_consistent(ti, pulled_task_id, pulled_key) -> Union[bool, None]:
    data_path = ti.xcom_pull(task_ids = pulled_task_id, key = pulled_key)
    
    with open(data_path, 'rb') as raw_input:
        data = pickle.load(raw_input)

    keys = None
    for ticker_data in data:
        for report in ticker_data["Weekly Adjusted Time Series"].values():
            if keys is None:
                keys = set(report.keys())
            else:
                report_keys = set(report.keys())
                if keys != report_keys:
                    error_message = 'The keys are not valid.'
                    logging.error(error_message)
                    raise ValueError(error_message)
    print('Keys are matching.')
    return True

def quarterly_metrics_are_consistent(ti, pulled_task_id, pulled_key) -> Union[bool, None]:
    data_path = ti.xcom_pull(task_ids = pulled_task_id, key = pulled_key)
    
    with open(data_path, 'rb') as raw_input:
        data = pickle.load(raw_input)

    keys = None
    for ticker_data in data:
        for report in ticker_data["quarterlyReports"]:
            if keys is None:
                keys = set(report.keys())
            else:
                report_keys = set(report.keys())
                if keys != report_keys:
                    error_message = 'The keys are not valid.'
                    logging.error(error_message)
                    raise ValueError(error_message)
    print('Keys are matching.')
    return True

# DATA TRANSFORMATION FUNCTIONS

def get_weekly_financials(ti, pulled_task_id, pulled_key, pushed_key) -> str:
    raw_input_path = ti.xcom_pull(task_ids = pulled_task_id, key = pulled_key)

    csv_file = f'processed_data_weekly_{datetime.datetime.now()}.csv'
    data_folder = create_output_folder('processed-data')
    csv_file_path = os.path.join(data_folder, csv_file)

    with open(raw_input_path, 'rb') as raw_input:
        raw_input = pickle.load(raw_input)

    keys = list(list(raw_input[0]['Weekly Adjusted Time Series'].values())[0].keys())

    with open(csv_file_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['data_as_of', 'symbol', 'date'] + keys)  
        for item in raw_input:
            symbol = item['Meta Data']['2. Symbol']
            weekly_data = item['Weekly Adjusted Time Series']
            for date, values in weekly_data.items():
                current_date = datetime.datetime.now().strftime('%Y-%m-%d')
                row = [current_date, symbol, date]
                row.extend([values.get(key) for key in keys])
                writer.writerow(row)
    print(f"CSV file '{csv_file}' has been created.")

    ti.xcom_push(key = pushed_key, value = csv_file_path)
    return csv_file_path

def get_quarterly_financials(ti, pulled_task_id, pulled_key, pushed_key) -> str:
    raw_input_path = ti.xcom_pull(task_ids = pulled_task_id, key = pulled_key)

    csv_file = f'processed_data_quarterly_{datetime.datetime.now()}.csv'
    data_folder = create_output_folder('processed-data')
    csv_file_path = os.path.join(data_folder, csv_file)

    with open(raw_input_path, 'rb') as raw_input:
        raw_input = pickle.load(raw_input)

    keys = list(raw_input[0]["quarterlyReports"][0].keys())

    with open(csv_file_path, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['data_as_of', 'symbol'] + keys)

        for ticker in raw_input:
            symbol = ticker['symbol']
            quarterly_reports = ticker['quarterlyReports']
            
            for report in quarterly_reports:
                report_values = list(report.values())
                current_date = datetime.datetime.now().strftime('%Y-%m-%d')
                row = [current_date, symbol] + report_values
                writer.writerow(row)

    print(f"The CSV file '{csv_file}' has been created.")
    ti.xcom_push(key = pushed_key, value = csv_file_path)
    return csv_file_path

# DATA LOADING FUNCTIONS

def load_to_s3(ti, pulled_task_id, pulled_key) -> bool:
    csv_file = ti.xcom_pull(task_ids = pulled_task_id, key = pulled_key)
    print('Accessing S3...')
    access_key = os.environ.get('AWS_ACCESS_KEY_ID')
    secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
    region = os.environ.get('AWS_REGION')
    bucket = str(os.environ.get('BUCKET'))
    file_name = csv_file

    #upload one file to keep historical data and one to be used in downstream tasks
    object_keys = [os.path.basename(csv_file), f"{os.path.basename(csv_file).split('_20')[0]}.csv"]
    
    if access_key is None or secret_key is None or region is None or bucket is None:
        error_message = 'One or more required S3 environment variables are missing.'
        logging.error(error_message)
        raise ValueError(error_message)
    
    try:
        s3 = boto3.client(
            service_name = 's3',
            region_name = region,
            aws_access_key_id = access_key,
            aws_secret_access_key = secret_key
        )
        print('Connected to S3.')
        try:
            for object_key in object_keys:
                s3.upload_file(file_name, bucket, object_key)
                print(f"File '{object_key}' uploaded to S3 bucket '{bucket}'.")
            return True
        except boto3.exceptions.S3UploadFailedError as e:
            logging.error('S3 upload failed:', e)
            return False
        except Exception as e:
            logging.error('An error occurred during S3 upload:', e)
            return False
    except botocore.exceptions.NoCredentialsError:
        logging.error('AWS credentials not found or not provided.')
        return False
    except botocore.exceptions.PartialCredentialsError:
        logging.error('Partial AWS credentials found or provided.')
        return False
    except botocore.exceptions.EndpointConnectionError:
        logging.error('Error connecting to the S3 endpoint.')
        return False
    except botocore.exceptions.NoRegionError:
        logging.error('AWS region not found or not provided.')
        return False
    except Exception as e:
        logging.error('An error occurred while connecting to S3:', e)
        return False
