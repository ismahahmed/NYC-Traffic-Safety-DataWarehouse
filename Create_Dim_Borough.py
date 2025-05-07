import pandas as pd
from sqlalchemy import create_engine, text
import config

def create_borough_dim():
    boroughs = ['BRONX', 'BROOKLYN', 'MANHATTAN', 'QUEENS', 'STATEN ISLAND']

    borough_df = pd.DataFrame({'borough': boroughs})

    conn_str = f"postgresql://{config.database_config['user']}:{config.database_config['password']}@" \
                f"{config.database_config['host']}:{config.database_config['port']}/{config.database_config['dbname']}"

    engine = create_engine(conn_str)

    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM boroughdim")) # gives error if i don't wrap in text
        count = result.scalar() 

    if count == 0:
        borough_df.to_sql('boroughdim', engine, if_exists='append', index=False)
        print("Inserted boroughs into boroughdim.")
    else:
        print("The table 'boroughdim' is not empty. No data was inserted.")

