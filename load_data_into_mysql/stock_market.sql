CREATE DATABASE stock;

USE stock;

CREATE TABLE Stocks (
    Symbol VARCHAR(10) PRIMARY KEY,
    CompanyName VARCHAR(100),
    Series VARCHAR(50),
    Industry VARCHAR(50),
    ISIN_Code VARCHAR(50)
);

CREATE TABLE Trades (
    ID INT AUTO_INCREMENT PRIMARY KEY,
    Date TIMESTAMP,
    Symbol VARCHAR(10),
    Prev_Close DECIMAL(10, 2),
    Open DECIMAL(10, 2),
    High DECIMAL(10, 2),
    Low DECIMAL(10, 2),
    Last DECIMAL(10, 2),
    Close DECIMAL(10, 2),
    VWAP DECIMAL(10, 2),
    Volume INT,
    Turnover BIGINT,
    Trades INT,
    Deliverable_Volume INT,
    Percent_Deliverable DECIMAL(5, 2),
    FOREIGN KEY (Symbol) REFERENCES Stocks(Symbol)
);