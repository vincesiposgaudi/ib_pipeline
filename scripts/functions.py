import os
import csv
import time
import boto3
import logging
import botocore
import requests
from typing import Union
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(filename='application.log', level=logging.ERROR)

def get_fincancials(tickers, function) -> list:
    av_key = os.environ.get('AV_KEY')

    if av_key is None:
        error_message = 'AV_KEY environment variable is missing.'
        logging.error(error_message)
        raise ValueError(error_message)
    
    raw_data = []
    for ticker in tickers:
        starttime = time.perf_counter()
        url = f'https://www.alphavantage.co/query?function={function}&symbol={ticker}&apikey={av_key}'
        try:
            response = requests.get(url)
            response.raise_for_status() 
        except requests.exceptions.RequestException as e:
            logging.error('Error occurred during the request: %s', e)
        except requests.exceptions.HTTPError as e:
            logging.error('HTTP Error occurred:', e)
        except Exception as e:
            logging.error('An unexpected error occurred:', e)
        endtime = time.perf_counter()
        data = response.json()
        raw_data.append(data)
        duration = round((endtime - starttime), 2)
        print(f'Getting the data for {ticker} was successful and it took {duration} seconds.')
        try:
            time.sleep(12 - duration) # 5 calls per minute for Alpha Vantage API - elapsed time of the request
        except ValueError as v:
            logging.error('The API response took more than a minute; no need for further wait.')
            pass
    return raw_data

def get_quarterly_financials(raw_input) -> str:
    csv_file = 'quarterly_data.csv'
    keys = list(raw_input[0]["quarterlyReports"][0].keys())

    with open(csv_file, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['symbol'] + keys)

        for ticker in raw_input:
            symbol = ticker['symbol']
            quarterly_reports = ticker['quarterlyReports']
            
            for report in quarterly_reports:
                report_values = list(report.values())
                row = [symbol] + report_values
                writer.writerow(row)

    print(f"CSV file '{csv_file}' has been created.")
    return csv_file

def quarterly_metrics_are_consistent(data) -> Union[bool, None]:
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

def get_weekly_financials(raw_input) -> str:
    csv_file = 'weekly_data.csv'
    keys = list(list(raw_input[0]['Weekly Adjusted Time Series'].values())[0].keys())

    with open(csv_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['symbol'] + keys)  
        for item in raw_input:
            symbol = item['Meta Data']['2. Symbol']
            weekly_data = item['Weekly Adjusted Time Series']
            for date, values in weekly_data.items():
                row = [symbol, date]
                row.extend([values.get(key) for key in keys])
                writer.writerow(row)
    print(f"CSV file '{csv_file}' has been created.")
    return csv_file

def weekly_metrics_are_consistent(data) -> Union[bool, None]:
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

def load_to_s3(csv_file) -> None:
    print('Accessing S3...')
    access_key = os.environ.get('AWS_ACCESS_KEY_ID')
    secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
    region = os.environ.get('AWS_REGION')
    bucket = str(os.environ.get('BUCKET'))
    file_name = csv_file

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
            s3.upload_file(file_name, bucket, file_name)
            print(f"File '{file_name}' uploaded to S3 bucket '{bucket}'.")
        except boto3.exceptions.S3UploadFailedError as e:
            logging.error('S3 upload failed:', e)
        except Exception as e:
            logging.error('An error occurred during S3 upload:', e)
    except botocore.exceptions.NoCredentialsError:
        logging.error('AWS credentials not found or not provided.')
    except botocore.exceptions.PartialCredentialsError:
        logging.error('Partial AWS credentials found or provided.')
    except botocore.exceptions.EndpointConnectionError:
        logging.error('Error connecting to the S3 endpoint.')
    except botocore.exceptions.NoRegionError:
        logging.error('AWS region not found or not provided.')
    except Exception as e:
        logging.error('An error occurred while connecting to S3:', e)

def delete_local_file(file_path):
    try:
        os.remove(file_path)
        print(f"File '{file_path}' deleted successfully.")
    except OSError as e:
        print(f"Error occurred while deleting file '{file_path}': {e}")