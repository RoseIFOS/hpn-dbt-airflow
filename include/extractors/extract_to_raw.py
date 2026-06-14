import os

import pandas as pd
from sqlalchemy import create_engine
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# As variáveis vêm do ambiente (no Astro, do arquivo .env injetado nos containers):
#   POSTGRES_CONN, SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD

TABLES = [
    "dbo.account", "dbo.account_header", "dbo.customer", "dbo.department_group",
    "dbo.finance", "dbo.geography", "dbo.organization", "dbo.product",
    "dbo.product_sub_category", "dbo.product_cost_history", "dbo.region",
    "dbo.sales_details", "dbo.sales_header", "dbo.sales_returns",
]


def get_pg_engine():
    return create_engine(os.environ["POSTGRES_CONN"])


def get_snow_conn():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse="LOADING_WH",
        database="HPN_DW",
        schema="RAW",
        role="LOADER_ROLE",
    )


def extract_and_load(table: str):
    engine = get_pg_engine()
    df = pd.read_sql(f"SELECT * FROM {table}", engine)

    # Adiciona metadata de carga
    df["_loaded_at"] = pd.Timestamp.now(tz="UTC")
    df["_source"] = "postgres_oltp"

    # Normaliza colunas para UPPER (padrão Snowflake)
    df.columns = [c.upper() for c in df.columns]

    # Remove o schema de origem (dbo.) para nomear a tabela RAW
    raw_table = f"RAW_{table.split('.')[-1].upper()}"

    snow = get_snow_conn()
    success, nchunks, nrows, _ = write_pandas(
        conn=snow,
        df=df,
        table_name=raw_table,
        auto_create_table=True,
        overwrite=True,
    )
    snow.close()
    print(f"[OK] {table}: {nrows} linhas carregadas")


if __name__ == "__main__":
    erros = []
    for t in TABLES:
        try:
            extract_and_load(t)
        except Exception as e:
            print(f"[ERRO] {t}: {e}")
            erros.append(t)
    if erros:
        print(f"\n{len(erros)} tabela(s) com erro: {erros}")
    else:
        print("\nTodas as tabelas carregadas com sucesso.")
