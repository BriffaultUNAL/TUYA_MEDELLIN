import time
import logging
import sys
import os
import yaml
import sqlalchemy as sa
from sqlalchemy import text, Engine, Connection, Table, VARCHAR
from pandas.io.sql import SQLTable
from urllib.parse import quote
import pandas as pd
from pandas import DataFrame
from src.telegram_bot import enviar_mensaje
import subprocess
import asyncio
import zipfile


act_dir = os.path.dirname(os.path.abspath(__file__))
proyect_dir = os.path.join(act_dir, '..')
sys.path.append(proyect_dir)

path_to_edgedriver = os.path.join(
    proyect_dir, 'edgedriver', 'msedgedriver.exe')
log_dir = os.path.join(proyect_dir, 'log', 'logs_main.log')
yml_credentials_dir = os.path.join(proyect_dir, 'config', 'credentials.yml')
# path_file = """Z:\REPORTING\6_Integrantes\81.Miguel Hernandez\PLANOS"""
path_file = os.path.join(
    'Z:', 'REPORTING', '6_Integrantes', '81.Miguel Hernandez', 'PLANOS')

logging.basicConfig(
    level=logging.INFO,
    filename=(log_dir),
    format="%(asctime)s - %(levelname)s -  %(message)s",
    datefmt='%d-%b-%y %H:%M:%S'
)


with open(yml_credentials_dir, 'r') as f:

    try:
        config = yaml.safe_load(f)
        source1 = config['source1']
        source2 = config['source2']
    except yaml.YAMLError as e:
        logging.error(str(e), exc_info=True)


class Engine_sql:

    def __init__(self, username: str, password: str, host: str, database: str, port: str = 3306) -> None:
        self.user = username
        self.passw = password
        self.host = host
        self.dat = database
        self.port = port

    def get_engine(self) -> Engine:
        return sa.create_engine(f"mysql+pymysql://{self.user}:{quote(self.passw)}@{self.host}:{self.port}/{self.dat}?autocommit=true")

    def get_connect(self) -> Connection:
        return self.get_engine().connect()


engine_61 = Engine_sql(**source1)


def to_sql_replace(table: SQLTable, con: Engine | Connection, keys: list[str], data_iter):

    satable: Table = table.table
    ckeys = list(map(lambda s: s.replace(' ', '_'), keys))
    data = [dict(zip(ckeys, row)) for row in data_iter]
    values = ', '.join(f':{nm}' for nm in ckeys)
    stmt = f"REPLACE INTO {satable.name} VALUES ({values})"
    con.execute(text(stmt), data)


def extract() -> DataFrame:
    try:
        asyncio.run(enviar_mensaje(f'Tuya Planos'))
        file_name = os.listdir(os.path.join(path_file))[0]
        file_name = os.path.join(path_file, file_name)
        df = pd.read_excel(file_name, header=0)
        # os.remove(file_name)
        print(df)
        return df
    except Exception as e:
        asyncio.run(enviar_mensaje(f'Error tuya_planos {e}'))
        logging.info(e)


def transform(df: DataFrame) -> DataFrame:
    try:
        df.columns = df.columns.str.replace(' ', '_')

        # df = df.drop('Unnamed:_104', axis=1)

        logging.info(df.columns)
        print(len(df))

        return df

    except Exception as e:
        asyncio.run(enviar_mensaje(f'Error tuya_planos {e}'))
        logging.info(e)


def load(df: DataFrame):
    try:
        with engine_61.get_connect() as conn:

            count = pd.read_sql_query(
                text(f"SELECT count(*) FROM bbdd_cs_med_tuya.tb_informe_planos"), conn)['count(*)'][0]

            conn.execute(text(
                f"delete from bbdd_cs_med_tuya.tb_informe_planos limit {count}"))

            logging.info(f"{count} datos en la tabla preproceso")

            df.to_sql('tb_informe_planos', con=conn,
                      if_exists="append", index=False)

            count_ = pd.read_sql_query(
                text(f"SELECT count(*) FROM bbdd_cs_med_tuya.tb_informe_planos"), conn)['count(*)'][0]

            logging.info(
                f"{len(df)} datos en dataframe - {count_} datos en tabla post-proceso")

            asyncio.run(enviar_mensaje(
                f'{len(df)} datos cargados \n {count_} datos en la tabla'))
            asyncio.run(enviar_mensaje(
                ("____________________________________")))
    except Exception as e:
        asyncio.run(enviar_mensaje(f'Error tuya_planos {e}'))
        logging.info(e)
