CREATE TABLE IF NOT EXISTS finance.stock_data (
    "data_as_of" VARCHAR,
    "symbol" VARCHAR,
    "date" VARCHAR,
    "open" REAL,
    "high" REAL,
    "low" REAL,
    "close" REAL,
    "adjusted_close" REAL,
    "volume" REAL,
    "dividend_amount" REAL
);