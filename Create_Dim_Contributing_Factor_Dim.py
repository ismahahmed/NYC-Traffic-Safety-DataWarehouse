import pandas as pd
import config   
from sqlalchemy import create_engine, text

def clean_data(crashes_df):

    cols = [
        'CONTRIBUTING FACTOR VEHICLE 1',
        'CONTRIBUTING FACTOR VEHICLE 2',
        'CONTRIBUTING FACTOR VEHICLE 3',
        'CONTRIBUTING FACTOR VEHICLE 4',
        'CONTRIBUTING FACTOR VEHICLE 5'
    ]

    # Standardize column names
    crashes_df.columns = crashes_df.columns.str.strip().str.upper()

    # Filter to include only columns that exist in the DataFrame
    existing_cols = [col for col in cols if col in crashes_df.columns]

    if not existing_cols:
        print("No contributing factor columns found in the DataFrame.")
        return crashes_df

    # Clean the existing columns
    crashes_df[existing_cols] = crashes_df[existing_cols].fillna('No Contributing Factor Details')
    crashes_df[existing_cols] = crashes_df[existing_cols].replace(['0', '1', '80', 'UNSPECIFIED'], 'No Contributing Factor Details')
    return crashes_df

def create_dim(crashes_df):
    contributing_factor_columns = [
        'CONTRIBUTING FACTOR VEHICLE 1',
        'CONTRIBUTING FACTOR VEHICLE 2',
        'CONTRIBUTING FACTOR VEHICLE 3',
        'CONTRIBUTING FACTOR VEHICLE 4',
        'CONTRIBUTING FACTOR VEHICLE 5'
    ]

    # Make cure column values are uppercase/full case
    crashes_df[contributing_factor_columns] = crashes_df[contributing_factor_columns].apply(lambda col: col.str.upper())

    # Replace NaN values with 'NO INFO'
    crashes_df[contributing_factor_columns] = crashes_df[contributing_factor_columns].fillna('NO INFO')

    cf_df = crashes_df[contributing_factor_columns]
    cf_stacked = cf_df.melt(value_name="contributing_factor")
    unique_contributing_factors = cf_stacked['contributing_factor'].drop_duplicates().reset_index(drop=True)
    unique_contributing_factors_df = pd.DataFrame({'contributing_factor': unique_contributing_factors})

    conn_str = f"postgresql://{config.database_config['user']}:{config.database_config['password']}@" \
               f"{config.database_config['host']}:{config.database_config['port']}/{config.database_config['dbname']}"

    engine = create_engine(conn_str)

    if unique_contributing_factors_df.empty:
        print("No unique contributing factors.")
        return(crashes_df)

    # Check if contributingfactors_dim table is empty
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM contributingfactor_dim"))
        count = result.scalar()

    if count == 0:
        unique_contributing_factors_df[['contributing_factor']].to_sql('contributingfactor_dim', engine, if_exists='append', index=False)
        print(f"Inserted {len(unique_contributing_factors_df)} rows into contributingfactor_dim.")
    else:
        print("contributingfactor_dim table is not empty. Skipping insert.")

    return crashes_df
 
def main(crashes_df):
    # Clean the data
    crashes_df = clean_data(crashes_df)

    # Create the dimension table
    crashes_df = create_dim(crashes_df)


    return(crashes_df)

