import pandas as pd
import re
import config


def clean_street_names(df, column_name):
    '''
    Purpose of Function: clean Street names in csv files
    Standard Format: Uppercase, No Punctuation, Removes Extra spaces (leading, trailing and any others)
    Street Abbreviations are being removed and replaced by full name (ex: st -> street)
    '''

    df['Street'] = df[column_name].fillna('')
    df[column_name] = df[column_name].fillna('').replace('NA', '')

    df['Street'] = (df[column_name]
        .str.strip()  
        .str.upper()  
        .str.replace(r'[^\w\s]', '', regex=True)  # remove extra punctuation
        .str.replace(r'\s+', ' ', regex=True)   # remove extra spaces
        .str.strip()  
                    
        .str.replace(r'^\s*N\s?', 'NORTH ', regex=True)  # replace N with NORTH
        .str.replace(r'^\s*S\s?', 'SOUTH ', regex=True)  #  replace S with SOUTH
        .str.replace(r'^\s*E\s?', 'EAST ', regex=True) #  replace E with EAST
        .str.replace(r'^\s*W\s?', 'WEST ', regex=True) # replace W with WEST
                    
        # Replace common street suffixes
        .str.replace(r"\bAVE\b$", "AVENUE", regex=True)  
        .str.replace(r"\bA\b$", "AVENUE", regex=True)  
        .str.replace(r"\bRD\b$", "ROAD", regex=True)
        .str.replace(r"\bBLVD\b$", "BOULEVARD", regex=True)
        .str.replace(r"\bPL\b$", "PLACE", regex=True)
        .str.replace(r"\bDR\b$", "DRIVE", regex=True)
        .str.replace(r"\bLN\b$", "LANE", regex=True)
        .str.replace(r"\bPKWY\b$", "PARKWAY", regex=True)
        .str.replace(r"\bCT\b$", "COURT", regex=True)  
        .str.replace(r"\bHWY\b$", "HIGHWAY", regex=True)
        .str.replace(r"\bCIR\b$", "CIRCLE", regex=True)
        .str.replace(r"\bSQ\b$", "SQUARE", regex=True)
        .str.replace(r"\bST\b$", "STREET", regex=True)
        .str.replace(r"\bSTR\b$", "STREET", regex=True)
        .str.replace(r"\bBLV\b$", "BOULEVARD", regex=True)
        .str.replace(r"\bBLVD\b$", "BOULEVARD", regex=True)
        .str.replace(r"\bRDWY\b$", "ROADWAY", regex=True)
        .str.replace(r"\bTERR\b$", "TERRACE", regex=True)
        .str.replace(r"\bTER\b$", "TERRACE", regex=True)

        # Correct Spelling
        .str.replace(r"\bPARKWY\b$", "PARKWAY", regex=True)
                    
        .str.replace(r'(?<=\d)(ST|ND|RD|TH)\b', '', regex=True) # removes the suffixes after street number ed: 22nd or 
        .str.replace(r'^\d+\s+(\d+.*)', r'\1', regex=True) # removes training numbers
        .str.replace(r'\d+$', '', regex = True)
        .str.replace(r'^\d+\s(\d+)', r'\1', regex=True)
                    
        .str.strip()
    )
    
    return df

def remove_bad_streets(df, column_name):
    df[column_name] = df[column_name].str.replace(r'^\d+$', '', regex=True)
    return df                                  

def correct_street_spell(df):
    for old, new in config.replace_street_suffix.items():
        df['Street'] = df['Street'].str.replace(rf"\b{old}\b$", new, regex=True)
    return df

def replace_street_name_by_coordinates(crashes_df, requests_df):
    unique_requests = requests_df[['FromLatitude', 'ToLongitude', 'Street']].drop_duplicates()

    # Iteratating through unique lat, lon pairs from requests_df. If found, replace street name
    for _, request_row in unique_requests.iterrows():
        lat, lon, street_name = request_row['FromLatitude'], request_row['ToLongitude'], request_row['Street']
        
        crashes_df.loc[(crashes_df['LATITUDE'] == lat) & (crashes_df['LONGITUDE'] == lon), 'Street'] = street_name

    return crashes_df

def clean_borough(df, col):
    df[col] = (df[col]
            .str.strip()  
            .str.upper())
    return(df)

def requests_zip_column(crashes_df, requests_df):
    '''
    Since requests_df does not have a zipcode column, we will create one merging location data in crash df
    '''
    zip_map = (
        crashes_df[['Street', 'BOROUGH', 'ZIP CODE']]
        .dropna(subset=['ZIP CODE'])
        .drop_duplicates(subset=['Street', 'BOROUGH'])
        .rename(columns={'BOROUGH': 'Borough'})
    )

    requests_df = requests_df.merge(
        zip_map,
        on=['Street', 'Borough'],
        how='left'  
    )

    return requests_df

def crashes_zip_column(crashes_df):
    '''
    Mapping crashes df with zip code from the same df (to itself). This way
    we can see if we can find zipcode for any missing values
    '''
    zip_map = (
    crashes_df[['Street', 'BOROUGH', 'ZIP CODE']]
    .dropna(subset=['ZIP CODE'])
    .drop_duplicates(subset=['Street', 'BOROUGH']))

    crashes_df = crashes_df.merge(
        zip_map,
        on=['Street', 'BOROUGH'],
        how='left',
        suffixes=('', '_mapped')
    )

    crashes_df['ZIP CODE'] = crashes_df['ZIP CODE'].fillna(crashes_df['ZIP CODE_mapped'])
    crashes_df.drop(columns=['ZIP CODE_mapped'], inplace=True)

    return crashes_df


def clean_all(crashes_df, requests_df):
    """
    Function to clean location data in crashes and requests dataframes.
    """
    crashes_df = clean_street_names(crashes_df, 'ON STREET NAME')
    requests_df = clean_street_names(requests_df, "OnStreet")
    crashes_df = remove_bad_streets(crashes_df, 'ON STREET NAME')
    crashes_df = correct_street_spell(crashes_df)
    crashes_df = replace_street_name_by_coordinates(crashes_df, requests_df)
    requests_df = clean_borough(requests_df,'Borough') 
    crashes_df = clean_borough(crashes_df, 'BOROUGH')

    requests_df = requests_zip_column(crashes_df, requests_df)
    crashes_df = crashes_zip_column(crashes_df)




    
    return crashes_df, requests_df
