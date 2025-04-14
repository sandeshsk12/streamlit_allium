import logging
import requests
from datetime import datetime
import snowflake.connector
import pandas as pd
import streamlit as st

# Set up logging
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

# Configuration dictionary for Snowflake connection using secrets.toml
SNOWFLAKE_CONFIG = {
    "user": st.secrets["snowflake"]["user"],
    "password": st.secrets["snowflake"]["password"],
    "account": st.secrets["snowflake"]["account"],
    "warehouse": "COMPUTE_WH",
    "database": "ALLIUM_ASSIGNMENT",
    "schema": "TOKEN_TRANSFERS"
}

def get_transfers(cur):
    sql = """
    SELECT * from EZ_TOKEN_TRANSFERS
    """
    try:
        # Execute the SQL query
        cur.execute(sql)
        
        # Fetch all the results
        results = cur.fetchall()
        df = pd.DataFrame(results)
        df.columns = [
            'blockchain',
            'block_timestamp',
            'block_number',
            'block_hash',
            'transaction_hash',
            'event_index',
            'from_address',
            'to_address',
            'token_address',
            'raw_amount',
            'amount',
            'amount_usd'
        ]
        return df
    except Exception as e:
        logger.error("Error loading token data: %s", e)
        raise

def main():
    # Streamlit app interface
    st.title("Token Transfers Data")
    st.write("This app retrieves token transfer data from Snowflake and displays it.")
    transfers = get_transfers(cur)

    # Button to fetch data
    st.button("Fetch Data")


if __name__ == "__main__":
    main()
