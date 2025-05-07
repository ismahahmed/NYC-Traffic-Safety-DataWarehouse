import pandas as pd
import config   
from sqlalchemy import create_engine, text

def create_dim(crashes_df):
    vehicle_columns = [
        'VEHICLE TYPE CODE 1',
        'VEHICLE TYPE CODE 2',
        'VEHICLE TYPE CODE 3',
        'VEHICLE TYPE CODE 4',
        'VEHICLE TYPE CODE 5'
    ]

    # Ensure the columns exist in the DataFrame
    valid_columns = [col for col in vehicle_columns if col in crashes_df.columns]
    if not valid_columns:
        print("No valid vehicle type columns found in crashes_df.")
        return pd.DataFrame({'vehicle_type': []})  # Return an empty DataFrame

    # Stack the vehicle type columns into a single column
    crashes_vehicle = crashes_df[valid_columns]
    crashes_vehicle_stacked = crashes_vehicle.melt(value_name="vehicle_type")
    crashes_vehicle_stacked['vehicle_type'] = crashes_vehicle_stacked['vehicle_type'].fillna('No_Vehicle')

    # Get unique vehicle types and convert to a DataFrame
    unique_vehicles = crashes_vehicle_stacked['vehicle_type'].unique()
    return pd.DataFrame({'vehicle_type': unique_vehicles})

def insert_to_dim(unique_vehicles):
    """
    Inserts unique vehicle types into the 'vehiclename' column of the vehicletypedim table in PostgreSQL
    only if the table is currently empty.
    """
    conn_str = f"postgresql://{config.database_config['user']}:{config.database_config['password']}@" \
               f"{config.database_config['host']}:{config.database_config['port']}/{config.database_config['dbname']}"

    engine = create_engine(conn_str)

    if unique_vehicles.empty:
        print("No unique vehicles to insert into vehicletypedim.")
        return

    # Check if vehicletypedim table is empty
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM vehicletypedim"))
        count = result.scalar()

    if count == 0:
        unique_vehicles = unique_vehicles.rename(columns={'vehicle_type': 'vehiclename'})
        unique_vehicles[['vehiclename']].to_sql('vehicletypedim', engine, if_exists='append', index=False)
        print(f"Inserted {len(unique_vehicles)} rows into vehicletypedim.")
    else:
        print("vehicletypedim table is not empty. Skipping insert.")


def main(crashes_df):
    unique_vehicles = create_dim(crashes_df)

    insert_to_dim(unique_vehicles)
