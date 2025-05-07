import pandas as pd
import re
import config
from sqlalchemy import create_engine, text

def create_requests_datetime_column(df, original_col, new_col):
    '''
    Function to create a new datetime column from an existing column
    Parameters:
    - df: DataFrame containing the original column
    - original_col: Name of the original column to convert
    - new_col: Name of the new datetime column to create
    '''
    df[new_col] = pd.to_datetime(df[original_col], errors='coerce')
    return df

def create_crash_datetime_column(df, date_col, time_col, new_col):
    '''
    Function to create a new datetime column (as date time types)
    Parameters:
    - df: DataFrame containing the original columns
    - date_col: Name of the date column
    - time_col: Name of the time column
    - new_col: Name of the new datetime column to create
    '''
    df[new_col] = pd.to_datetime(df[date_col] + ' ' + df[time_col], errors='coerce')
    return df
    
def extract_datetime_parts(df, datetime_col):
    '''
    Function to extract parts of a datetime column and create new columns
    Parameters:
    - df: DataFrame containing the datetime column
    - datetime_col: Name of the datetime column to extract parts from
    '''
    col_suffix = datetime_col  
    df[f'Month_{col_suffix}'] = df[datetime_col].dt.month.astype('Int64')
    df[f'Day_{col_suffix}'] = df[datetime_col].dt.day.astype('Int64')
    df[f'Year_{col_suffix}'] = df[datetime_col].dt.year.astype('Int64')
    df[f'Hour_{col_suffix}'] = df[datetime_col].dt.hour.astype('Int64')
    df[f'Minute_{col_suffix}'] = df[datetime_col].dt.minute.astype('Int64')
    df[f'Second_{col_suffix}'] = df[datetime_col].dt.second.astype('Int64')
    return df

def get_unique_time_columns(df, cols):
    '''
    Function to extract unique time columns from a DataFrame
    Parameters:
    - df: DataFrame containing the datetime columns
    - cols: List of columns to extract
    '''
    year_cols = [col for col in cols if 'Year' in col]
    month_cols = [col for col in cols if 'Month' in col]
    day_cols = [col for col in cols if 'Day' in col]

    hour_cols = [col for col in cols if 'Hour' in col]
    minute_cols = [col for col in cols if 'Minute' in col]
    second_cols = [col for col in cols if 'Second' in col]
    
    combined = pd.DataFrame({
        'Year': df[year_cols].stack().reset_index(drop=True),
        'Month': df[month_cols].stack().reset_index(drop=True),
        'Day': df[day_cols].stack().reset_index(drop=True),
        'Hour': df[hour_cols].stack().reset_index(drop=True),
        'Minute': df[minute_cols].stack().reset_index(drop=True),
        'Second': df[second_cols].stack().reset_index(drop=True)
    })
    
    unique_combined = combined.drop_duplicates().reset_index(drop=True)
    
    return unique_combined

def insert_into_postgres(dim_df, postgres_table, id_column):
    '''
    Function to insert data into PostgreSQL table.
    Parameters: 
    - dim_df: DataFrame to be inserted
    - postgres_table: Name of the PostgreSQL table
    - id_column: Column name to check for existing IDs
    '''
    conn_str = f"postgresql://{config.database_config['user']}:{config.database_config['password']}@" \
               f"{config.database_config['host']}:{config.database_config['port']}/{config.database_config['dbname']}"

    engine = create_engine(conn_str)

    with engine.connect() as conn:
        try: # if table exists, check for existing IDs
            existing_ids = pd.read_sql(f"SELECT {id_column} FROM {postgres_table}", conn)[id_column].tolist()
        except Exception as e: # if table does not exist, create it
            print(f"Table {postgres_table} does not exist. Creating it.")
            dim_df.to_sql(postgres_table, engine, if_exists='replace', index=False)
            return

    dim_df = dim_df[~dim_df[id_column].isin(existing_ids)] # removing rows that already exist in the table

    if not dim_df.empty:
        dim_df.to_sql(postgres_table, engine, if_exists='append', index=False)
        print(f"Inserted {len(dim_df)} new rows into {postgres_table}.")
    else:
        print(f"No new rows to insert into {postgres_table}.") # if all ID's are already in the table, nothing to insert


def main(requests_df, crashes_df):
    
    requests_df = requests_df.drop('SecondStudyCode', axis=1) # removing this column since 'Second' is in the name- this creates an error down the line

    # Creating datetime columns for the 1 column in crash and 4 columns in requests
    crashes_df = create_crash_datetime_column(crashes_df, 'CRASH DATE', 'CRASH TIME', 'Crash_DateTime')
    requests_df = create_requests_datetime_column(requests_df, 'DateAdded', 'DateTime_DateAdded')
    requests_df = create_requests_datetime_column(requests_df, 'InstallationDate', 'DateTime_InstallationDate')
    requests_df = create_requests_datetime_column(requests_df, 'ClosedDate', 'DateTime_ClosedDate')
    requests_df = create_requests_datetime_column(requests_df, 'RequestDate', 'DateTime_RequestDate')

    # Extracting date and time parts from the datetime columns, for example, Year_DateTime_DateAdded and Month_DateTime_DateAdde
    requests_df = extract_datetime_parts(requests_df, 'DateTime_DateAdded')
    requests_df = extract_datetime_parts(requests_df, 'DateTime_ClosedDate')
    requests_df = extract_datetime_parts(requests_df, 'DateTime_RequestDate')
    requests_df = extract_datetime_parts(requests_df, 'DateTime_InstallationDate')
    crashes_df = extract_datetime_parts(crashes_df, 'Crash_DateTime')

    # Getting all column names from the DataFrames
    crashes_columns = crashes_df.columns
    requests_columns = requests_df.columns

    # Extracting unique time columns from the DataFrames, this is based on if Month, Day, Year, Hour, Minute, Second are in the column name
    requests_unique_times_df = get_unique_time_columns(requests_df, requests_columns)
    crashes_unique_times_df = get_unique_time_columns(crashes_df, crashes_columns)

    # Combining the unique time columns from both DataFrames as they will be using the same dimensional tables
    combined_df = pd.concat([requests_unique_times_df , crashes_unique_times_df])
    unique_df = combined_df.drop_duplicates().reset_index(drop=True)

    # Dropping duplicates and resetting the index
    unique_date_df = unique_df[['Year', 'Month', 'Day']].drop_duplicates().reset_index(drop=True)
    unique_time_df = unique_df[['Hour', 'Minute', 'Second']].drop_duplicates().reset_index(drop=True)
    # Sorting the DataFrames by Year, Month, Day and Hour, Minute, Second
    unique_date_df = unique_date_df.sort_values(by=['Year', 'Month', 'Day']).reset_index(drop=True)
    unique_time_df = unique_time_df.sort_values(by=['Hour', 'Minute', 'Second']).reset_index(drop=True)

    # Prep to create new ID column
    DateDim = unique_date_df.reset_index()
    TimeDim = unique_time_df.reset_index()

    # Creating the DateDim DataFrame with Date_Dim_ID
    DateDim = unique_date_df.reset_index(drop=True)
    DateDim['date_dim_id'] = DateDim.apply(
        lambda row: (f"DATE{int(row['Year']):04}{int(row['Month']):02}{int(row['Day']):02}"), axis=1)
    
    DateDim['date'] = pd.to_datetime(DateDim[['Year', 'Month', 'Day']]).dt.strftime('%Y-%m-%d')

    DateDim.columns = [col.lower() for col in DateDim.columns]

    # Creating the TimeDim DataFrame with Time_Dim_ID
    TimeDim = unique_time_df.reset_index(drop=True)
    TimeDim['time_dim_id'] = TimeDim.apply(
        lambda row: (f"TIME{int(row['Hour']):02}{int(row['Minute']):02}{int(row['Second']):02}"), axis=1
    )

    TimeDim['time'] = TimeDim.apply(
        lambda row: f"{int(row['Hour']):02}:{int(row['Minute']):02}:{int(row['Second']):02}", axis=1
    )

    # Ensure column names are lowercase before inserting into PostgreSQL
    TimeDim.columns = [col.lower() for col in TimeDim.columns]
    DateDim.columns = [col.lower() for col in DateDim.columns]

    # Inserting into PostgreSQL
    insert_into_postgres(TimeDim, 'timedim', 'time_dim_id')
    insert_into_postgres(DateDim, 'datedim', 'date_dim_id')

    # print(DateDim.head())
    # print(TimeDim.head())

    return(crashes_df, requests_df)


    
