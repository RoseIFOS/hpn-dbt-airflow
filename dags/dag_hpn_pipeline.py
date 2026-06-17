"""
Pipeline ELT do HPN DW.

Fluxo: extrai as tabelas do Postgres OLTP para o schema RAW do Snowflake,
depois roda o dbt (staging -> intermediate -> marts) via Cosmos.
"""

from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task
from airflow.models import Variable
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.constants import TestBehavior
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

# Caminhos dentro do container do Astro
DBT_PROJECT_PATH = Path("/usr/local/airflow/dbt/hpn-dbt-dw/dw_hpn")
EXTRACTORS_PATH = "/usr/local/airflow/include/extractors"

# Ambiente alvo do dbt (dev/prod) — controlado pela Variable do Airflow.
# Modelo de compute híbrido: a extração roda sempre no LOADING_WH (compartilhado);
# só o transform muda de warehouse por ambiente.
DBT_ENV = Variable.get("dbt_env", default_var="dev")
ENV_CONFIG = {
    "dev":  {"schema": "DBT_DEV", "warehouse": "TRANSFORM_DEV_WH"},
    "prod": {"schema": "PROD",    "warehouse": "TRANSFORM_PROD_WH"},
}
env = ENV_CONFIG[DBT_ENV]

# Profile do dbt gerado a partir da conexão snowflake_default do Airflow.
# A conexão fornece account/user/password; o resto vem dos profile_args.
profile_config = ProfileConfig(
    profile_name="dw_hpn",
    target_name=DBT_ENV,
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_default",
        profile_args={
            "database": "HPN_DW",
            "schema": env["schema"],
            "warehouse": env["warehouse"],
            "role": "DBT_ROLE",
        },
    ),
)


@dag(
    dag_id="hpn_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="0 6 * * *",  # todo dia às 6h UTC
    catchup=False,
    tags=["hpn", "elt"],
    doc_md=__doc__,
)
def hpn_pipeline():

    @task
    def extract_postgres():
        import sys
        sys.path.insert(0, EXTRACTORS_PATH)
        from extract_to_raw import TABLES, extract_and_load

        erros = []
        for t in TABLES:
            try:
                extract_and_load(t)
            except Exception as e:
                print(f"[ERRO] {t}: {e}")
                erros.append(t)
        if erros:
            raise RuntimeError(f"Falha ao carregar: {erros}")

    dbt_build = DbtTaskGroup(
        group_id="dbt_build",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=ExecutionConfig(dbt_executable_path="/usr/local/bin/dbt"),
        # AFTER_ALL: builda todos os modelos primeiro e roda os testes no fim.
        # Evita o deadlock do AFTER_EACH em schema novo, onde o relationship
        # fct_sales -> dim_* roda no grupo da dim antes de a fct existir.
        render_config=RenderConfig(test_behavior=TestBehavior.AFTER_ALL),
        operator_args={"install_deps": True},  # roda dbt deps antes de cada modelo
    )

    extract_postgres() >> dbt_build


hpn_pipeline()
