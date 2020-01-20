import pandas as pd
import snowflake.connector
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine


df = pd.read_csv("<sample_data>.csv")

engine = create_engine(
    URL(
        account="<Hidden>",
        user="<Hidden>",
        password="<Hidden>",
        database="delivery",
        schema="data",
        warehouse="compute_wh",
        role="sysadmin",
    )
)

connection = engine.connect()

df.to_sql("raw_data", con=engine, index=False, if_exists="replace", chunksize=10000)

connection.close()
engine.dispose()
