import pandas as pd
import config   
from sqlalchemy import create_engine

def yearmonthdim():

    conn_str = f"postgresql://{config.database_config['user']}:{config.database_config['password']}@" \
                    f"{config.database_config['host']}:{config.database_config['port']}/{config.database_config['dbname']}"

    engine = create_engine(conn_str)

    # data from datedim table
    with engine.connect() as conn:
        datedim_df = pd.read_sql("SELECT DISTINCT year, month FROM datedim", conn)

    datedim_df['month'] = datedim_df['month'].astype(str).str.upper()
    # create month_year_id
    datedim_df['month_year_id'] = datedim_df['month'].str.upper() + "_" + datedim_df['year'].astype(str)

    # get existing month_year_id from monthyeardim table
    with engine.connect() as conn:
        existing_monthyeardim = pd.read_sql("SELECT month_year_id FROM monthyeardim", conn)

    # filter out existing month_year_id in monthyeardim
    new_monthyeardim_df = datedim_df[~datedim_df['month_year_id'].isin(existing_monthyeardim['month_year_id'])]

    # insert new records into monthyeardim
    if not new_monthyeardim_df.empty:
        new_monthyeardim_df = new_monthyeardim_df[['month_year_id', 'month', 'year']]  # Select only necessary columns
        new_monthyeardim_df.to_sql('monthyeardim', engine, if_exists='append', index=False)
        print(f"Inserted {len(new_monthyeardim_df)} new records into monthyeardim.")
    else:
        print("No new records to insert into monthyeardim.")