import pandas as pd
import warnings
import re
import config
import Clean_Location
import Clean_Vehicle
import Create_Dim_Date
import Create_Dim_Street
import Create_Dim_Borough
import Create_Dim_VehicleType
import Create_Dim_MonthYear
import Create_Dim_Contributing_Factor_Dim
import Create_Dim_Requests
import Write_To_Postgres

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", message="Could not infer format, so each element will be parsed individually")

def drop_request_col(df):
    '''
    Purpose of Function: Drop columns that are not needed for the analysis.
    '''
    columns_to_drop = [
        "CBLetterRequestDate", "CBLetterRecievedDate", "RequestorLetterReplyDate",
        "BCTSNum", "CCUNum", "MarkingsDate", "OldSign", "NewSign",
        "LIONKey", "OFT", "CB", "OldSign1", "NewSign1", "OldSign2", "NewSign2"
    ]

    df = df.drop(columns=columns_to_drop)

    return df

def drop_duplicates(df):
    num_duplicates = df.duplicated(subset=None).sum()
    df_cleaned = df.drop_duplicates(subset=None)
    return df_cleaned

def drop_requests_duplicates(df):
    df = df.drop_duplicates(subset=['ProjectCode', 'SegmentID'], keep='first')
    return(df)


def main():
    crashes_raw = pd.read_csv("raw_data/Motor_Vehicle_Collisions_Crashes.csv", low_memory = False)
    sr_requests_raw = pd.read_csv("raw_data/Speed_Reducer_Tracking_System.csv")

    crashes_df = crashes_raw
    requests_df = sr_requests_raw

    requests_df = drop_request_col(requests_df)
    
    # DROPPING DUPLICATES
    print("\nDuplicate rows in SRTS:", sr_requests_raw.duplicated().sum())
    print("Duplicate rows in Crashes:", crashes_raw.duplicated().sum())
    crashes_df = drop_duplicates(crashes_df)
    requests_df = drop_duplicates(requests_df)
    requests_df = drop_requests_duplicates(requests_df)

    print("\nDuplicate rows in SRTS after dropping duplicates:", requests_df.duplicated().sum())
    print("Duplicate rows in Crashes after dropping duplicates:", crashes_df.duplicated().sum())

    # Cleaning Location Data
    crashes_df, requests_df = Clean_Location.clean_all(crashes_df, requests_df)

    # Cleaning Vehicle Data
    crashes_df = Clean_Vehicle.main(crashes_df)

    # Zip Codes
    print("\nPercentage of Zipcodes found in Requests DataFrame: ", requests_df['ZIP CODE'].notna().sum() / len(requests_df) * 100)
    print("Percentage of Zipcodes found in Crashes DataFrame: ", crashes_df['ZIP CODE'].notna().sum() / len(crashes_df) * 100)
    print("\n")
    
    print("Dimension Creation/Updates")
    crashes_df, requests_df = Create_Dim_Date.main(requests_df, crashes_df)
    Create_Dim_MonthYear.yearmonthdim()
    Create_Dim_Street.main(requests_df, crashes_df)
    Create_Dim_Borough.create_borough_dim()
    Create_Dim_VehicleType.main(crashes_df)
    crashes_df = Create_Dim_Contributing_Factor_Dim.main(crashes_df)
    requests_df = Create_Dim_Requests.main(requests_df)

    Write_To_Postgres.write_to_postgres(crashes_df, requests_df)


    

if __name__ == "__main__":
    main()
