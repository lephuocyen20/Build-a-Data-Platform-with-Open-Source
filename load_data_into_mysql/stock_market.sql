CREATE DATABASE stock;

USE stock;

CREATE TABLE companies (
    symbol VARCHAR(10) PRIMARY KEY,
    comGroupCode VARCHAR(10),
    organName NVARCHAR(100),
    organShortName NVARCHAR(100),
    organTypeCode VARCHAR(10),
    icbName NVARCHAR(100),
    sector NVARCHAR(100),
    industry NVARCHAR(100),
    group NVARCHAR(100),
    subGroup NVARCHAR(100),
    icbCode INT
);

CREATE TABLE Trades (
    ID INT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(10),
    value INT,
    tradingDate DATE,
    time TIMESTAMP,
    open INT,
    high INT,
    low INT,
    close INT,
    volume INT,
    FOREIGN KEY (symbol) REFERENCES companies(symbol)
);