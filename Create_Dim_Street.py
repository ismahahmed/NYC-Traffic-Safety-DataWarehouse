import pandas as pd
import re
import config
from sqlalchemy import create_engine, text


def create_dim(requests_df, crashes_df):
    '''
    1: Including Columns needed: StreetName, Borough, ZipCode
    3: Merging dataframes to include only unique locations
    4: Dropping duplicates
    '''

    request_street = requests_df[['RequestDate', 'Street', 'Borough', 'ZIP CODE', 'TrafficDirectionDesc']]
    request_street = request_street.dropna(subset=['Street', 'Borough', 'ZIP CODE', 'TrafficDirectionDesc', 'RequestDate'])
    request_street = request_street.drop_duplicates()
    request_street['RequestDate'] = pd.to_datetime(request_street['RequestDate'])
    request_street = request_street.sort_values(by=['Street', 'Borough', 'ZIP CODE', 'RequestDate'])

    requests_dim_street = request_street.groupby(['Street', 'Borough', 'ZIP CODE']).agg(
        traffic_direction_previous=('TrafficDirectionDesc', lambda x: x.iloc[0]), # First value
        traffic_direction_current=('TrafficDirectionDesc', lambda x: x.iloc[-1]) # Last value
        ).reset_index()
    
    # If traffic_direction_previous equals traffic_direction_current, set traffic_direction_previous to empty
    requests_dim_street['traffic_direction_previous'] = requests_dim_street.apply(
        lambda row: '' if row['traffic_direction_previous'] == row['traffic_direction_current'] else row['traffic_direction_previous'],
        axis=1
    )

    crashes_df_streets = crashes_df[['Street', 'BOROUGH', 'ZIP CODE']].drop_duplicates()
    crashes_df_streets = crashes_df_streets.rename(columns={'BOROUGH': 'Borough'})  # Correct renaming

    # Getting crashes street data that does not already exist in requests_dim_street
    crashes_dim_street = crashes_df_streets.merge( 
        requests_dim_street[['Street', 'Borough', 'ZIP CODE']],
        on=['Street', 'Borough', 'ZIP CODE'],
        how='left', 
        indicator=True
        ).query('_merge == "left_only"').drop(columns=['_merge'])
    # Since crashes does not have traffic direction, we will set the traffic direction column in records we did not find in requests_dim_street to None
    crashes_dim_street['traffic_direction_previous'] = None
    crashes_dim_street['traffic_direction_current'] = None

    streetdim = pd.concat([requests_dim_street, crashes_dim_street], ignore_index=True)

    streetdim = streetdim.fillna('')
    
    streetdim['StreetID'] = (
    'STREETID_' +
    streetdim['Street'].str.replace(' ', '_').str.upper() + '_' +
    streetdim['Borough'].str.replace(' ', '_').str.upper() + '_' +
    streetdim['ZIP CODE'].astype(str))

    streetdim = streetdim.reset_index(drop=True)
    #streetdim['Street_Dim_Id'] = streetdim.index + 1

    streetdim = streetdim.rename(columns={
    'Street': 'streetname',
    'ZIP CODE': 'zipcode',
    'Borough': 'borough',
    'StreetID': 'streetid'
    })

    return streetdim


def apply_scd_type3(existing_df, new_df):
    """
    Returns:
        - inserts_df: rows to be inserted (new streetid)
        - updates_df: rows to be updated (existing streetid, changed direction)
    """
    existing_df['streetid'] = existing_df['streetid'].astype(str)
    new_df['streetid'] = new_df['streetid'].astype(str)

    merged = new_df.merge(
        existing_df[['streetid', 'traffic_direction_current']],
        on='streetid',
        how='left',
        suffixes=('', '_in_postgres')
    )

    inserts = []
    updates = []

    for _, row in merged.iterrows():
        if pd.isna(row['traffic_direction_current_in_postgres']):
            inserts.append({
                'streetid': row['streetid'],
                'streetname': row['streetname'],
                'borough': row['borough'],
                'zipcode': row['zipcode'],
                'traffic_direction_previous': row['traffic_direction_previous'],
                'traffic_direction_current': row['traffic_direction_current']
            })
        else:
            if row['traffic_direction_current'] != row['traffic_direction_current_in_postgres']:
                updates.append({
                    'streetid': row['streetid'],
                    'traffic_direction_previous': row['traffic_direction_current_in_postgres'],
                    'traffic_direction_current': row['traffic_direction_current']
                })

    return pd.DataFrame(inserts), pd.DataFrame(updates)

def update_streetdim_scd_type3(requests_df, crashes_df):
    """
    Update the street dimension table using SCD Type 3 logic.
    Args:
        requests_df: DataFrame containing request data.
        crashes_df: DataFrame containing crash data.
    """
    engine = create_engine(f"postgresql://{config.database_config['user']}:{config.database_config['password']}@{config.database_config['host']}:{config.database_config['port']}/{config.database_config['dbname']}")

    # Step 1: Create new dimension DataFrame
    new_dim_df = create_dim(requests_df, crashes_df)

    # Step 2: Read existing dimension data from the database
    existing_dim_df = get_records_from_postgres('streetdim')

    # Step 3: Apply SCD Type 3 logic
    inserts_df, updates_df = apply_scd_type3(existing_dim_df, new_dim_df)

    # Step 4: Insert new records
    if not inserts_df.empty: # if there are new records to insert
        inserts_df.to_sql('streetdim', engine, if_exists='append', index=False)
        print(f"Inserted {len(inserts_df)} new rows.")

    # Step 5: Update changed records
    if not updates_df.empty: # if there are records to update
        with engine.begin() as conn:
            for _, row in updates_df.iterrows():
                conn.execute(text("""
                    UPDATE streetdim
                    SET traffic_direction_previous = :previous,
                        traffic_direction_current = :current
                    WHERE streetid = :id
                """), {
                    'previous': row['traffic_direction_previous'],
                    'current': row['traffic_direction_current'],
                    'id': row['streetid']
                })
        print(f"Updated {len(updates_df)} rows.")

    if inserts_df.empty and updates_df.empty:
        print("No changes detected. Dimension is up to date.")

def get_records_from_postgres(table_name):
    # Database connection
    engine = create_engine(f"postgresql://{config.database_config['user']}:{config.database_config['password']}@{config.database_config['host']}:{config.database_config['port']}/{config.database_config['dbname']}")
    with engine.connect() as conn:
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, conn)
    return df
   

def insert_into_postgres(df, table_name):
    # Database connection
    engine = create_engine(f"postgresql://{config.database_config['user']}:{config.database_config['password']}@{config.database_config['host']}:{config.database_config['port']}/{config.database_config['dbname']}")
    
    df.to_sql(table_name, engine, if_exists='append', index=False)

    print(f"Data successfully inserted into {table_name}!")


def main(requests_df, crashes_df):
    update_streetdim_scd_type3(requests_df, crashes_df)
    

    


    


