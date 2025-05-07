import pandas as pd
import re
import config
from sqlalchemy import create_engine, text

vehicle_columns = [
    'VEHICLE TYPE CODE 1',
    'VEHICLE TYPE CODE 2',
    'VEHICLE TYPE CODE 3',
    'VEHICLE TYPE CODE 4',
    'VEHICLE TYPE CODE 5'
]

vehicle_map_df = pd.read_csv('vehicle_mapping.csv')
vehicle_map_dict = pd.Series(vehicle_map_df['Convert To'].values, index=vehicle_map_df['Vehicle_Type'].values).to_dict()

def replace_vehicle_type(value):
    # if there are no matches for value in dictionary, return 'UNKNOWN'
    if pd.isna(value):
        return 'UNKNOWN'
    return vehicle_map_dict.get(value, value)

def main(crashes_df):
    for col in vehicle_columns:
        crashes_df[col] = crashes_df[col].apply(replace_vehicle_type)
        crashes_df[col] = crashes_df[col].fillna('No_Vehicle')  
    return crashes_df





