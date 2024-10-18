from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
import functions_framework
from google.cloud import secretmanager
import duckdb

# settings
project_id = 'ba882-435919'
secret_id = 'motherduck_access_token'   #<---------- this is the name of the secret you created
version_id = 'latest'

# db setup
db = 'stock_market'
schema = "stage"
db_schema = f"{db}.{schema}"

@functions_framework.http
def task(request):
    # instantiate the services 
    sm = secretmanager.SecretManagerServiceClient()

    # Build the resource name of the secret version
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

    # Access the secret version
    response = sm.access_secret_version(request={"name": name})
    md_token = response.payload.data.decode("UTF-8")

    # initiate the MotherDuck connection through an access token
    md = duckdb.connect(f'md:?motherduck_token={md_token}') 

    # create the database
    create_db_sql = f"CREATE DATABASE IF NOT EXISTS {db};"   
    md.sql(create_db_sql)

    # create the schema
    md.sql(f"CREATE SCHEMA IF NOT EXISTS {db_schema};") 

    # create the tables based on the ERD

# stock table
    stock_tbl_sql = f"""
    CREATE TABLE IF NOT EXISTS {db_schema}.stock (
        id INTEGER PRIMARY KEY,
        ticker VARCHAR(10) NOT NULL,
        name VARCHAR(100) NOT NULL,
        industry VARCHAR(100) NOT NULL,
        marketCap INTEGER NOT NULL, 
        currency VARCHAR(5) NOT NULL
    );
    """
    md.sql(stock_tbl_sql)

# price table
    price_tbl_sql = f"""
    CREATE TABLE IF NOT EXISTS {db_schema}.price (
        price_id VARCHAR PRIMARY KEY,
        ticker VARCHAR(10) NOT NULL,
        timestamp TIMESTAMP NOT NULL,
        open DOUBLE NOT NULL,
        high DOUBLE NOT NULL,
        low DOUBLE NOT NULL,
        close DOUBLE NOT NULL,
        volume INTEGER NOT NULL
    );
    """
    md.sql(price_tbl_sql)

# dividend table
    dividend_tbl_sql = f"""
    CREATE TABLE IF NOT EXISTS {db_schema}.dividend (
        dividend_id VARCHAR PRIMARY KEY,
        ticker VARCHAR(10) NOT NULL,
        timestamp TIMESTAMP NOT NULL,
        dividend DOUBLE NOT NULL
    );
    """
    md.sql(dividend_tbl_sql)

# financials table
    financials_tbl_sql = f"""
    CREATE TABLE IF NOT EXISTS {db_schema}.financials (
        financials_id VARCHAR PRIMARY KEY,
        ticker VARCHAR(10) NOT NULL,
        date TIMESTAMP NOT NULL,
        revenue DOUBLE,
        profit DOUBLE,
        debt DOUBLE, 
        assets DOUBLE, 
        eps DOUBLE
    );
    """
    md.sql(financials_tbl_sql)

# SEC table
    SEC_tbl_sql = f"""
    CREATE TABLE IF NOT EXISTS {db_schema}.SEC (
        sec_id VARCHAR PRIMARY KEY,
        ticker VARCHAR(10) NOT NULL,
        date TIMESTAMP NOT NULL,
        title VARCHAR(100),
        primary_url VARCHAR(100),
        secondary_url VARCHAR(100), 
    );
    """
    md.sql(SEC_tbl_sql)

# News table
    news_tbl_sql = f"""
    CREATE TABLE IF NOT EXISTS {db_schema}.news (
        uuid VARCHAR PRIMARY KEY,
        ticker VARCHAR(10) NOT NULL,
        article_url VARCHAR(100) NOT NULL,
        published_date TIMESTAMP,
        title VARCHAR(100),
        publisher VARCHAR(100), 
        news_type VARCHAR(100), 
        thumbnail_url VARCHAR(100)
    );
    """
    md.sql(news_tbl_sql)
