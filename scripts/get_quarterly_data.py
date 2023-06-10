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
        ib_income_statements = get_fincancials(tickers, 'INCOME_STATEMENT')
        if quarterly_metrics_are_consistent(ib_income_statements):
            quarterly_reports = get_quarterly_financials(ib_income_statements)
            load_to_s3(quarterly_reports)
            delete_local_file(quarterly_reports)
    except Exception as e:
        logging.error('An error occurred:', e)



