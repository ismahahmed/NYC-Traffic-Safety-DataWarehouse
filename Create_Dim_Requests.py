import pandas as pd
import config   
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import Clean_Data

import warnings
warnings.filterwarnings('ignore')


def clean_requests(requests_df):
    '''
    Creates a unique identifier for each request and maps the project status to a status category
        - param requests_df: DataFrame containing request data
        - return: DataFrame with cleaned data
    '''

    requests_df['RequestId'] = requests_df['ProjectCode'].astype(str) + '_' + requests_df['SegmentID'].astype(str)
    
    # Iterate Over config.status_mapping, loop through each category and its relevant statuses 
    # Map Each Status to Its Category
    reverse_mapping = {
        status: category for category, statuses in config.status_mapping.items() # dictionary in config file
        for status in statuses}
    
    requests_df['StatusCategory'] = requests_df['ProjectStatus'].map(reverse_mapping).fillna('Other')
    return(requests_df)

def check_if_requests_dim_empty():
    '''
    Check of postgre table `requestdim` is empty
        - return: True if empty, False if its not
    '''
    conn_str = f"postgresql://{config.database_config['user']}:{config.database_config['password']}@" \
                f"{config.database_config['host']}:{config.database_config['port']}/{config.database_config['dbname']}"
    engine = create_engine(conn_str)
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM requestdim"))
        count = result.scalar()
    return count == 0

def insert_all_data(scd_request_dim):
    '''
    Insert all data if check_if_requests_dim_empty() is True
        - param scd_request_dim: DataFrame containing request data
        - return: None
    '''
    scd_request_dim.loc[:, 'effective_start_date'] = pd.to_datetime(datetime.today().date())
    scd_request_dim.loc[:, 'effective_end_date'] = '9999-12-31'
    scd_request_dim.loc[:, 'current_flag'] = True
    conn_str = f"postgresql://{config.database_config['user']}:{config.database_config['password']}@" \
                    f"{config.database_config['host']}:{config.database_config['port']}/{config.database_config['dbname']}"
    engine = create_engine(conn_str)
    scd_request_dim.to_sql('requestdim', engine, if_exists='append', index=False)
    print(f"Inserted {len(scd_request_dim)} rows into requestdim.")

def update_data(scd_request_dim):
    """
    Update the requestdim table in PostgreSQL with SCD Type 2 logic
        - param scd_request_dim: DataFrame containing request data
        - return: Nothing, just updates and prints out the number of rows inserted
    """
    # Connect to PostgreSQL
    conn_str = f"postgresql://{config.database_config['user']}:{config.database_config['password']}@" \
               f"{config.database_config['host']}:{config.database_config['port']}/{config.database_config['dbname']}"
    engine = create_engine(conn_str)

    # Getting existing current data from the requestdim table (flag = True)
    with engine.connect() as conn:
        existing_data = pd.read_sql("SELECT * FROM requestdim WHERE current_flag = TRUE", conn) # existing current data 

    # Merge existing data with new data
    merged = pd.merge(
        existing_data,
        scd_request_dim,
        on="requestid",
        how="right",
        suffixes=('_old', '_new'),
        indicator=True
    )

    # Rows that need to be updated (These are rows where status has changed but the requestid is the same)
    rows_to_update = merged[
        (merged['_merge'] == 'both') & (
            (merged['projectstatus_old'] != merged['projectstatus_new']) |
            (merged['statuscategory_old'] != merged['statuscategory_new'])
        )
    ]

    # Right_only rows -> These are new requests that do not exist in the existing data (new requestids)
    rows_to_insert = merged[merged['_merge'] == 'right_only']

    # updates rows in rows_to_update making sure to set the current_flag to False and effective_end_date to today
    with engine.begin() as conn:
        for _, row in rows_to_update.iterrows():
            conn.execute(text("""
                UPDATE requestdim
                SET effective_end_date = :end_date,
                    current_flag = FALSE
                WHERE requestid = :requestid AND current_flag = TRUE
            """), {
                'end_date': datetime.today().date(),
                'requestid': row['requestid']
            })

    # prep new rows to insert (the maintance columns)
    new_rows = scd_request_dim[scd_request_dim['requestid'].isin(rows_to_update['requestid']) | scd_request_dim['requestid'].isin(rows_to_insert['requestid'])]
    new_rows['effective_start_date'] = datetime.today().date()
    new_rows['effective_end_date'] = '9999-12-31'
    new_rows['current_flag'] = True

    # insert new rows into the requestdim table
    new_rows.to_sql('requestdim', engine, if_exists='append', index=False)
    print(f"Inserted {len(new_rows)} rows into requestdim.")



def main(requests_df):
    requests_df = clean_requests(requests_df) # adds maintance columns to requests_df and StatusCategory column
    scd_request_dim = requests_df[['RequestId', 'ProjectCode', 'SegmentID', 'ProjectStatus', 'StatusCategory']]
    scd_request_dim.columns = scd_request_dim.columns.str.lower()

    # if the requestdim table is empty, insert all data- no need to go through the SCD process
    if check_if_requests_dim_empty():
        insert_all_data(scd_request_dim)
        return(requests_df) 
    
    # if it is not empty, we need to go through the SCD process
    else:
        update_data(scd_request_dim)
        
    return(requests_df)

# NEW DATA SCD TYPE 2 TESTING
def new_request_data():
    crashes_raw = pd.read_csv("raw_data/Motor_Vehicle_Collisions_Crashes.csv", low_memory = False)
    sr_requests_raw = pd.read_csv("raw_data/NewRequestsData.csv")

    crashes_df = crashes_raw
    requests_df = sr_requests_raw

    # Cleaning Location Data
    crashes_df, requests_df = Clean_Data.Clean_Location.clean_all(crashes_df, requests_df)

    requests_df = clean_requests(requests_df) 
    scd_request_dim = requests_df[['RequestId', 'ProjectCode', 'SegmentID', 'ProjectStatus', 'StatusCategory']]
    scd_request_dim.columns = scd_request_dim.columns.str.lower()

    # if the requestdim table is empty, insert all data- no need to go through the SCD process
    if check_if_requests_dim_empty():
        insert_all_data(scd_request_dim)
        print("Inserted all data into requestdim table.")
        return(requests_df) 

    # if it is not empty, we need to go through the SCD process
    else:
        update_data(scd_request_dim)
        print("Updated requestdim table with new data.")

    return(requests_df)

# requests_df = new_request_data()