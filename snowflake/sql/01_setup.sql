-- ============================================================
-- Healthcare Fraud Detection Project
-- File: 01_setup.sql
-- Author: Prudhvi Keerthi
-- Date: March 2026
-- Description: Complete Snowflake environment setup including
--              warehouse, database, schemas, stage, file format,
--              tables, and data loading from AWS S3
-- ============================================================


-- ────────────────────────────────────────────────────────────
-- SECTION 1: WAREHOUSE SETUP
-- What: Creates the computing engine for running queries
-- Why:  Snowflake separates storage from compute. The warehouse
--       provides the processing power to run SQL queries.
--       X-SMALL is the cheapest size — sufficient for this project.
--       AUTO_SUSPEND saves credits by turning off after 60 seconds
--       of inactivity. AUTO_RESUME turns it back on automatically.
-- ────────────────────────────────────────────────────────────

CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE;


-- ────────────────────────────────────────────────────────────
-- SECTION 2: DATABASE SETUP
-- What: Creates the main database container for this project
-- Why:  A database is the top-level object in Snowflake that
--       holds all schemas and tables. One database per project
--       keeps everything organized and isolated.
-- ────────────────────────────────────────────────────────────

CREATE DATABASE IF NOT EXISTS HEALTHCARE_FRAUD;


-- ────────────────────────────────────────────────────────────
-- SECTION 3: SCHEMA CREATION
-- What: Creates three logical layers inside the database
-- Why:  Schemas organize data by purpose:
--       RAW        → data loaded exactly as received from S3
--       ANALYTICS  → cleaned and transformed data for analysis
--       ML_FEATURES → model-ready features for XGBoost training
-- ────────────────────────────────────────────────────────────

CREATE SCHEMA IF NOT EXISTS HEALTHCARE_FRAUD.RAW;
CREATE SCHEMA IF NOT EXISTS HEALTHCARE_FRAUD.ANALYTICS;
CREATE SCHEMA IF NOT EXISTS HEALTHCARE_FRAUD.ML_FEATURES;

-- Verify all schemas were created (should show 5 including system schemas)
SHOW SCHEMAS IN DATABASE HEALTHCARE_FRAUD;


-- ────────────────────────────────────────────────────────────
-- SECTION 4: SET WORKING CONTEXT
-- What: Tells Snowflake which warehouse, database, and schema
--       to use by default for this session
-- Why:  Snowflake is stateless — context resets every session.
--       Setting context allows shorthand references in queries
--       instead of typing full paths every time.
--       NOTE: Run these three lines at the start of every session.
-- ────────────────────────────────────────────────────────────

-- Set the computing engine
USE WAREHOUSE COMPUTE_WH;

-- Set the active database
USE DATABASE HEALTHCARE_FRAUD;

-- Set the active schema
USE SCHEMA RAW;


-- ────────────────────────────────────────────────────────────
-- SECTION 5: EXTERNAL STAGE CREATION
-- What: Creates a named connection between Snowflake and S3
-- Why:  Snowflake cannot read S3 directly — it needs a stage
--       object that stores the S3 address and credentials.
--       Think of it as a loading dock between S3 and Snowflake.
-- Note: AWS credentials are placeholders — never commit real
--       keys to GitHub. Store real keys in .env file only.
-- ────────────────────────────────────────────────────────────

CREATE OR REPLACE STAGE HEALTHCARE_FRAUD.RAW.s3_stage
    URL = 's3://healthcare-fraud-raw-prudhvi/'
    CREDENTIALS = (
        AWS_KEY_ID = 'your_AWS_ACCESS_KEY_ID_here'
        AWS_SECRET_KEY = 'your_AWS_SECRET_ACCESS_KEY_here'
    )
    FILE_FORMAT = (
        TYPE = 'CSV'
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        SKIP_HEADER = 1
    );

-- Verify stage can see files in S3 bucket
-- Expected: two files listed (prescribers CSV and LEIE CSV)
LIST @HEALTHCARE_FRAUD.RAW.s3_stage;


-- ────────────────────────────────────────────────────────────
-- SECTION 6: FILE FORMAT CREATION
-- What: Creates a named, reusable file format object
-- Why:  Instead of repeating CSV settings in every query,
--       we save them once as a named object and reference it.
--       PARSE_HEADER reads the first row as column names.
--       ERROR_ON_COLUMN_COUNT_MISMATCH allows flexible columns.
-- ────────────────────────────────────────────────────────────

CREATE OR REPLACE FILE FORMAT HEALTHCARE_FRAUD.RAW.csv_format
    TYPE = 'CSV'
    PARSE_HEADER = TRUE
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    TRIM_SPACE = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;


-- ────────────────────────────────────────────────────────────
-- SECTION 7: SCHEMA INFERENCE
-- What: Scans actual CSV files in S3 to detect column names
--       and data types automatically
-- Why:  Instead of guessing column names, we let Snowflake read
--       the file header and tell us exactly what columns exist.
--       This prevents column count mismatch errors during loading.
--       Always run INFER_SCHEMA before creating tables.
-- ────────────────────────────────────────────────────────────

-- Check column metadata for CMS prescribers file (22 columns)
SELECT *
FROM TABLE(
    INFER_SCHEMA(
        LOCATION => '@HEALTHCARE_FRAUD.RAW.s3_stage/prescribers/',
        FILE_FORMAT => 'HEALTHCARE_FRAUD.RAW.csv_format'
    )
);

-- Check column metadata for LEIE fraud labels file (18 columns)
SELECT *
FROM TABLE(
    INFER_SCHEMA(
        LOCATION => '@HEALTHCARE_FRAUD.RAW.s3_stage/reference/',
        FILE_FORMAT => 'HEALTHCARE_FRAUD.RAW.csv_format'
    )
);


-- ────────────────────────────────────────────────────────────
-- SECTION 8: TABLE CREATION
-- What: Creates empty table structures to receive loaded data
-- Why:  Tables must exist before data can be loaded into them.
--       All columns are VARCHAR to ensure 100% of rows load
--       successfully. Blank numeric values cause errors if
--       columns are typed as NUMBER during initial load.
--       Data types are enforced later in the ANALYTICS schema
--       during the cleaning and transformation step.
-- ────────────────────────────────────────────────────────────

-- Table 1: CMS Medicare Part D Prescribers 2023
-- Source: data.cms.gov
-- Rows: 26,794,878
-- Contains prescribing patterns for every Medicare provider in the US
CREATE OR REPLACE TABLE HEALTHCARE_FRAUD.RAW.prescribers (
    Prscrbr_NPI                 VARCHAR,   -- National Provider Identifier (unique doctor ID)
    Prscrbr_Last_Org_Name       VARCHAR,   -- Provider last name or organization name
    Prscrbr_First_Name          VARCHAR,   -- Provider first name
    Prscrbr_City                VARCHAR,   -- Provider city
    Prscrbr_State_Abrvtn        VARCHAR,   -- State abbreviation (e.g. TX, CA)
    Prscrbr_State_FIPS          VARCHAR,   -- State FIPS code (numeric state identifier)
    Prscrbr_Type                VARCHAR,   -- Provider specialty type
    Prscrbr_Type_Src            VARCHAR,   -- Source of provider type classification
    Brnd_Name                   VARCHAR,   -- Brand name of the drug prescribed
    Gnrc_Name                   VARCHAR,   -- Generic name of the drug prescribed
    Tot_Clms                    VARCHAR,   -- Total number of claims
    Tot_30day_Fills             VARCHAR,   -- Total 30-day fills
    Tot_Day_Suply               VARCHAR,   -- Total days supply
    Tot_Drug_Cst                VARCHAR,   -- Total drug cost in USD
    Tot_Benes                   VARCHAR,   -- Total number of beneficiaries
    GE65_Sprsn_Flag             VARCHAR,   -- Suppression flag for 65+ beneficiaries
    GE65_Tot_Clms               VARCHAR,   -- Total claims for 65+ beneficiaries
    GE65_Tot_30day_Fills        VARCHAR,   -- Total 30-day fills for 65+ beneficiaries
    GE65_Tot_Drug_Cst           VARCHAR,   -- Total drug cost for 65+ beneficiaries
    GE65_Tot_Day_Suply          VARCHAR,   -- Total days supply for 65+ beneficiaries
    GE65_Bene_Sprsn_Flag        VARCHAR,   -- Suppression flag for 65+ bene count
    GE65_Tot_Benes              VARCHAR    -- Total 65+ beneficiaries
);

-- Table 2: LEIE Fraud Exclusions (February 2026)
-- Source: oig.hhs.gov
-- Rows: 82,749
-- Contains providers banned from Medicare for fraud, waste, or abuse
-- NPI column used to match against prescribers table to create fraud labels
CREATE OR REPLACE TABLE HEALTHCARE_FRAUD.RAW.leie_exclusions (
    FIRSTNAME               VARCHAR(100),  -- Provider first name
    LASTNAME                VARCHAR(100),  -- Provider last name
    MIDNAME                 VARCHAR(100),  -- Provider middle name
    BUSNAME                 VARCHAR(200),  -- Business/organization name
    GENERAL                 VARCHAR(100),  -- General category (e.g. Individual, Other Business)
    SPECIALTY               VARCHAR(100),  -- Provider specialty
    UPIN                    VARCHAR(20),   -- Unique Physician Identification Number (legacy)
    NPI                     VARCHAR(20),   -- National Provider Identifier (join key with prescribers)
    DOB                     VARCHAR(20),   -- Date of birth
    ADDRESS                 VARCHAR(200),  -- Street address
    CITY                    VARCHAR(100),  -- City
    STATE                   VARCHAR(10),   -- State abbreviation
    ZIP                     VARCHAR(20),   -- ZIP code
    EXCLTYPE                VARCHAR(50),   -- Type of exclusion
    EXCLDATE                VARCHAR(20),   -- Date exclusion began
    REINDATE                VARCHAR(20),   -- Reinstatement date (if applicable)
    WAIVERDATE              VARCHAR(20),   -- Waiver date (if applicable)
    WVRSTATE                VARCHAR(10)    -- Waiver state (if applicable)
);

-- Verify both tables were created successfully
SHOW TABLES IN SCHEMA HEALTHCARE_FRAUD.RAW;


-- ────────────────────────────────────────────────────────────
-- SECTION 9: LOAD DATA FROM S3
-- What: Copies CSV files from S3 stage into Snowflake tables
-- Why:  COPY INTO is Snowflake's bulk loading command — much
--       faster than row-by-row inserts. Reads directly from S3.
--       ON_ERROR = CONTINUE skips bad rows instead of stopping
--       the entire load — critical for large government datasets
--       that may have formatting inconsistencies.
-- ────────────────────────────────────────────────────────────

-- Load CMS prescribers data (takes 5-10 minutes, 3.9GB file)
-- Expected rows loaded: 26,794,878
COPY INTO HEALTHCARE_FRAUD.RAW.prescribers
FROM @HEALTHCARE_FRAUD.RAW.s3_stage/prescribers/
FILE_FORMAT = (
    TYPE = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1
)
ON_ERROR = 'CONTINUE';

-- Diagnose any load errors (run after COPY INTO if needed)
-- SELECT * FROM TABLE(VALIDATE(prescribers, JOB_ID => '_last'));

-- Load LEIE fraud labels (fast, 15MB file)
-- Expected rows loaded: 82,749
COPY INTO HEALTHCARE_FRAUD.RAW.leie_exclusions
FROM @HEALTHCARE_FRAUD.RAW.s3_stage/reference/
FILE_FORMAT = (
    TYPE = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1
)
ON_ERROR = 'CONTINUE';


-- ────────────────────────────────────────────────────────────
-- SECTION 10: VERIFICATION QUERIES
-- What: Confirms data loaded correctly into both tables
-- Why:  Always verify after loading — never assume success.
--       Count confirms all rows are present.
--       SELECT LIMIT 5 confirms real data is visible.
-- ────────────────────────────────────────────────────────────

-- Verify row counts
SELECT COUNT(*) FROM HEALTHCARE_FRAUD.RAW.prescribers;      -- Expected: 26,794,878
SELECT COUNT(*) FROM HEALTHCARE_FRAUD.RAW.leie_exclusions;  -- Expected: 82,749

-- Preview first 5 rows of each table
SELECT * FROM HEALTHCARE_FRAUD.RAW.prescribers LIMIT 5;
SELECT * FROM HEALTHCARE_FRAUD.RAW.leie_exclusions LIMIT 5;