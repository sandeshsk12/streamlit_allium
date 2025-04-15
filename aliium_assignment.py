import logging
import requests
from datetime import datetime
import snowflake.connector
import pandas as pd
import streamlit as st
import plotly.express as px

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

def get_transfers(cur, start_date=None, end_date=None):
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

@st.cache_data
def load_data():
    with snowflake.connector.connect(**SNOWFLAKE_CONFIG) as conn:
        with conn.cursor() as cur:
            df = get_transfers(cur)
            df['block_timestamp'] = pd.to_datetime(df['block_timestamp'])
            return df

def main():
    # Streamlit app interface
    st.title('Blockchain Transaction Analytics 📈')
    st.text('Transactions and Volume(USD) of xAUT')
    st.text('Refreshed hourly')

    

    
    
    df = load_data()
    
    # Create hourly aggregates
    hourly = df.set_index('block_timestamp').resample('H').agg({
        'amount_usd': 'sum',
        'transaction_hash': 'nunique'
    }).reset_index().rename(columns={
        'amount_usd': 'Total USD',
        'transaction_hash': 'Transaction Count'
    })

    # Create dual-axis plot
    fig = px.bar(hourly, x='block_timestamp', y='Transaction Count', title='Hourly Metrics')
    fig.add_scatter(x=hourly['block_timestamp'], y=hourly['Total USD'], 
                    mode='lines', name='USD Volume', yaxis='y2',
                    line=dict(color='red'))

    fig.update_layout(
        yaxis=dict(title='Transactions', showgrid=False),
        yaxis2=dict(title='USD Volume', overlaying='y', side='right'),
        hovermode='x unified'
    )

    st.plotly_chart(fig, use_container_width=True)
   

if __name__ == "__main__":
    main()
