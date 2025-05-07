# ENTER USER'S DATABASE CONFIGURATION BELOW 
database_config = {
    'host': '', 
    'port': ,
    'user': '',
    'password': '',
    'dbname': ''
}

replace_street_suffix = replace_dict = {
    "AAVENUE": "AVENUE","AUTHORIT": "AUTHORITY","AV": "AVENUE","AVANUE": "AVENUE","AVEN": "AVENUE",
    "AVEN4E": "AVENUE","AVENE": "AVENUE","AVENU": "AVENUE","AVENUUE": "AVENUE","AVEUNE": "AVENUE","AVNEUE": "AVENUE",
    "AVNUE": "AVENUE","AVUENUE": "AVENUE", "BBLVD": "BOULEVARD","BBROADWAY": "BROADWAY","BEAC": "BEACH","BEACH": "BEACH",
    "BL": "BLVD","BLBD": "BOULEVARD","BLOUVARD": "BOULEVARD","BLVDS": "BOULEVARD","BLVS": "BOULEVARD","BOILEVARD": "BOULEVARD",
    "BOLUEVARD": "BOULEVARD","BOU": "BOULEVARD","BOULEVARD": "BOULEVARD","BOULVARD": "BOULEVARD","BOULVEARD": "BOULEVARD",
    "BOUVELARD": "BOULEVARD","CONC" : "CONCOURSE","CONCOURS" : "CONCOURSE","CR" : "CREEK","CRES" : "CRESCENT","E": "EAST",
    "EB":"EASTBOUND","ENTRAN" : "ENTRANCE","EXP" : "EXPRESSWAY","EXPRE" : "EXPRESSWAY","EXPRESS" : "EXPRESSWAY", "EXPY" : "EXPRESSWAY",
    "EXPRESSAY": "EXPRESSWAY","EXPRESSWAAY": "EXPRESSWAY","EXPRESWAY": "EXPRESSWAY","EXPWAY": "EXPRESSWAY","EXPWY": "EXPRESSWAY",
    "EXPY": "EXPRESSWAY","EXRESSWAY": "EXPRESSWAY",
    "SREET": "STREET", "STEEET":"STREET", "STEET": "STREET", "STRE": "STREET", "STREE":"STREET", "STREEET":"STREET", "STREET":"STREET",
    "STREETQ":"STREET","STREETT":"STREET","STREEY":"STREET","STRET":"STREET", "STRRET": "STREET",
    'WB':'WESTBOUND','WEBSTER':'WESTBOUND','WEST':'WESTBOUND','WESTBO':'WESTBOUND'
}

# For Requests Dim and Data
status_mapping = {
    'Approved_and_Installed': [
        'Signs Installed', 'Study Completed, Closed', 'Installation Cancel, Closed'
    ],
    'Approved_Waiting_Installation': [
        'Sent approved queue for prioritization', 'BC prioritized installation queue',
        'Accept planning proposal of Feasible'
    ],
    'Request_In_Review': [
        'Planning proposal - Feasible', 'Planning proposal - Not Feasible', 
        'Study Assigned to Inspector', 'Study created', 'Study request passed to planning', 
        'Project on-hold', '2nd Inspection requested'
    ],
    'Denied': [
        'Planning denied Pre-inspection, Closed', 'Reject planning proposal of Feasible, Closed', 
        'Reject planning proposal of Not Feasible - 2nd inspection requested, Closed', 
        'Denied pre-inspection - Closed', 'Legacy: Reducer denied, Requestor reply letter sent - Closed', 
        'MOSAICS closed', 'Accept planning proposal of Not Feasible, Closed'
    ]
}

