import os
import csv
import time
import boto3
import logging
import botocore
import requests
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(filename='application.log', level=logging.ERROR)

def quarterly_metrics_are_consistent(data) -> bool:
    keys = set(data[0]["quarterlyReports"][0].keys())
    for ticker_data in data:
        for report in ticker_data["quarterlyReports"]:
            report_keys = set(report.keys())
            if keys != report_keys:
                print('Keys are not matching.')
                return False
            else:
                print('Keys are matching.')
                return True

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