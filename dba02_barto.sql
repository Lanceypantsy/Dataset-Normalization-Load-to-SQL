-- IPPS Part I
-- Database Assignment #2
-- Created at: 10/27/2019
-- Author: Lance Barto

-- Create and Select the db for use
DROP DATABASE IF EXISTS ipps;

CREATE DATABASE ipps;

USE ipps;

-- Create user, grant access
DROP USER IF EXISTS 'ipps';

CREATE USER 'ipps' IDENTIFIED BY 'password';
GRANT ALL ON ipps.* TO 'ipps';


-- Implement relational model

-- Found the longest string in the DRG description to be 68 characters, added some length buffer
CREATE TABLE DRG (
    drgID   INT         PRIMARY KEY,
    drgDesc VARCHAR(80) NOT NULL
);


-- Found the longest string in the hrrCity column to be 21 charactes, added some length buffer
CREATE TABLE HRR (
    hrrID       INT         PRIMARY KEY,
    hrrCity     VARCHAR(25) NOT NULL,
    hrrState    VARCHAR(2)  NOT NULL
);


-- Found the longest string in the Provider Name column to be 50 characters, added some length buffer
-- Found the longest string in the Provider Address column to be 44 characters, added some length buffer
-- Found the longest string in the Provider City column to be 15 characters, added some length buffer
CREATE TABLE Providers (
    provID      INT                 PRIMARY KEY,
    provName    VARCHAR(55)         NOT NULL,
    provAddress VARCHAR(50)         NOT NULL,
    provCity    VARCHAR(20)         NOT NULL,
    provState   VARCHAR(2)          NOT NULL,
    provZip     INT                 NOT NULL,
    hrrID       INT                 NOT NULL,
    FOREIGN KEY (hrrID) REFERENCES HRR(hrrID)
);

-- Found the longest number 
CREATE TABLE Payments (
    payID           INT              PRIMARY KEY,
    provID          INT,
    drgID           INT,
    dCount          INT              NOT NULL,
    covCharges      DECIMAL(9, 2)    NOT NULL,
    totalPaym       DECIMAL(9, 2)    NOT NULL,
    medicarePaym    DECIMAL(9, 2)    NOT NULL,
    FOREIGN KEY (provID)    REFERENCES Providers(provID),
    FOREIGN KEY (drgID)     REFERENCES DRG(drgID)
);
