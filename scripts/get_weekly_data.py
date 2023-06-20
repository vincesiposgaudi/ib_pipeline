from functions import *

tickers = [
    'BAC',    # Bank of America Merrill Lynch
    'BX',     # Blackstone
    'C',      # Citi
    'DB',     # Deutsche Bank
    'GS',     # Goldman Sachs
    'HSBC',   # HSBC
    'JPM',    # J.P. Morgan Chase
    'MS',     # Morgan Stanley
    'UBS'     # UBS
]

if __name__ == '__main__':
    try:
        stock_data = get_fincancials(tickers, 'TIME_SERIES_WEEKLY_ADJUSTED')
        if weekly_metrics_are_consistent(stock_data):
            weekly_reports = get_weekly_financials(stock_data)
            load_to_s3(weekly_reports)
            delete_local_file(weekly_reports)
    except Exception as e:
        logging.error('An error occurred:', e)



