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

    # User input for data range (optional)
    st.sidebar.header("Filters")
    start_date = st.sidebar.date_input("Start date", datetime(2023, 1, 1))
    end_date = st.sidebar.date_input("End date", datetime(2023, 12, 31))

    # Button to fetch data
    if st.button("Fetch Data"):
        try:
            # Establish a connection using a context manager
            with snowflake.connector.connect(**SNOWFLAKE_CONFIG) as conn:
                with conn.cursor() as cur:
                    transfers = get_transfers(cur)
                    st.write(f"Showing data between {start_date} and {end_date}")

                    # Filter data based on the selected date range
                    transfers['block_timestamp'] = pd.to_datetime(transfers['block_timestamp'])
                    filtered_transfers = transfers[
                        (transfers['block_timestamp'] >= pd.to_datetime(start_date)) &
                        (transfers['block_timestamp'] <= pd.to_datetime(end_date))
                    ]
                    
                    # Display filtered data in a table
                    st.dataframe(filtered_transfers)

        except Exception as e:
            st.error(f"An error occurred: {e}")
            logger.error("An error occurred during database operations: %s", e)

if __name__ == "__main__":
    main()
