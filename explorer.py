
#%%
import os, io
import pandas as pd
from sqlalchemy import create_engine
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from pathlib import Path
import dotenv

# sobe um nível (de hpn-dbt-airflow) e entra em hpn-dbt-dw
env_path = Path(__file__).resolve().parent.parent / "hpn-dbt-dw" / ".env"
dotenv.load_dotenv(env_path)

# %%

def get_pg_engine():
    url = os.environ["POSTGRES_CONN"]
    return create_engine(url)

# %%
table = "dbo.sales_details"

#%%
def extract_and_load(table: str):
    engine = get_pg_engine()
    df = pd.read_sql(f"SELECT * FROM {table}", engine)
    return df
# %%
df = extract_and_load(table)
df.head()
# %%

# %%
