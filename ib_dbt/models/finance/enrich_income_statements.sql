{{ config(
  alias='income_statements_w_stock_prices'
) }}

/*get the latest load from the source tables*/

WITH max_stock_data AS (
    SELECT 
        MAX(data_as_of) AS data_as_of
    FROM finance.stock_data
), 

max_income_statement_data AS (
    SELECT 
        MAX(data_as_of) AS data_as_of
    FROM finance.income_statement
),

/*aggregate the weekly stock data to quarterly*/

quarterly_stock_data AS (
    SELECT
        data_as_of AS stock_data_as_of,
        DATE_PART('year', TO_DATE(date, 'YYYY-MM-DD')) AS year,
        DATE_PART('quarter', TO_DATE(date, 'YYYY-MM-DD')) AS quarter,
        symbol,
        AVG(adjusted_close) AS avg_adjusted_close
    FROM finance.stock_data
    WHERE data_as_of = 
        (SELECT 
            data_as_of
        FROM max_stock_data)
    GROUP BY 1, 2, 3, 4
),

/*join the stock data to the income statements*/

enriched_data AS (
    SELECT
        i.*,
        q.stock_data_as_of,
        q.avg_adjusted_close
    FROM finance.income_statement i
    LEFT JOIN quarterly_stock_data q
    ON i.symbol = q.symbol
    AND DATE_PART('year', TO_DATE(i.fiscal_date_ending, 'YYYY-MM-DD')) = q.year
    AND DATE_PART('quarter', TO_DATE(i.fiscal_date_ending, 'YYYY-MM-DD')) = q.quarter
    WHERE i.data_as_of = 
            (SELECT 
                data_as_of
            FROM max_income_statement_data)
)

SELECT
    *
FROM enriched_data
