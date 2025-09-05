import os
import glob
import pandas as pd
from sqlalchemy import create_engine

URL = os.getenv("POSTGRES_URL", "postgresql+psycopg2://airflow:airflow@postgres/airflow")

def load_all(schema="dw"):
    engine = create_engine(URL, future=True)
    if schema:
        with engine.begin() as cn:
            cn.exec_driver_sql(f'CREATE SCHEMA IF NOT EXISTS "{schema}";')

    for file in glob.glob("/opt/airflow/data/processed/*.csv"):
        table = os.path.splitext(os.path.basename(file))[0]
        df = pd.read_csv(file)
        df.to_sql(table, engine, schema=schema, if_exists="replace", index=False)
        print(f"✅ Loaded {file} into {schema}.{table}")

if __name__ == "__main__":
    load_all()
