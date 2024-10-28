from selenium import webdriver
from selenium.webdriver.edge.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains


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
from datetime import datetime


act_dir = os.path.dirname(os.path.abspath(__file__))
proyect_dir = os.path.join(act_dir, '..')
sys.path.append(proyect_dir)

path_to_edgedriver = os.path.join(
    proyect_dir, 'edgedriver', 'msedgedriver.exe')
log_dir = os.path.join(proyect_dir, 'log', 'logs_main.log')
yml_credentials_dir = os.path.join(proyect_dir, 'config', 'credentials.yml')

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


def webscraping(import_username: str, import_password: str, *_):

    try:
        options = Options()
        options = webdriver.EdgeOptions()
        options.use_chromium = True
        options.add_argument('--ignore-certificate-errors')
        options.add_argument("--disable-popup-blocking")
        options.add_argument("--disable-infobars")
        options.add_argument("--disable-notifications")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--start-maximized")
        # options.add_argument("--headless")
        options.add_experimental_option("prefs", {
            "download.default_directory": os.path.join(act_dir, 'file')
        })

        service = Service(path_to_edgedriver)
        driver = webdriver.Edge(service=service, options=options)

        action_chains = ActionChains(driver)

        asyncio.run(enviar_mensaje(f'Tuya Prejuridico vigente'))

        subprocess.run(['cmd', '/c', 'del', '/Q',
                        os.path.join(act_dir, 'file') + '\\*'])

        driver.get("https://172.226.124.46/")

        time.sleep(5)
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "usuario"))
        ).click()

        user = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "usuario")))

        password = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "clave")))

        user.send_keys(import_username)
        password.send_keys(import_password)
        password.send_keys(Keys.ENTER)

        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "//*[@id='dialog']/div[3]/table/tbody/tr[6]/td[2]/a"))).click()

        driver.get(
            """https://172.226.124.46/vsmart/index.php?rtr=informes&ctr=InformesControlador&acc=&nom_programa=informesprejuridicos_smart""")

        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, "select2-container"))).click()

        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "//*[@id='select2-drop']/ul/li[2]/div"))).click()

        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "add_tab"))).click()

        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "/html/body/div[2]/div[1]/div/div[3]/div/div[2]/div[2]/div[1]/div/div[2]/div[2]/table/tbody/tr[1]/td[1]/label"))).click()

        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, "select2-container"))).click()

        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "//*[@id='select2-drop']/ul/li[144]"))).click()

        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "add_tab"))).click()

        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "i.fa-list-alt.fa-4x.colorAzulOscuro"))).click()

        element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "//*[@id='1']/td[4]")))

        action_chains.double_click(element).perform()

        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.XPATH, "/html/body/div[2]/div[1]/div/div[3]/div/div[2]/div[3]/div/div[2]/a[3]"))).click()

        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.XPATH, "/html/body/div[2]/div[1]/div/div[3]/div/div[2]/div[3]/div/div/span[1]/a[3]"))).click()

        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.XPATH, "/html/body/div[2]/div[1]/div/div[3]/div/div[2]/div[3]/div/div/div[2]/form/div[3]/input[1]"))).click()

        #########################
        time.sleep(30)
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "//*[@id='top-menu']/div[3]/div[6]/a/div"))).click()
        try:
            alert = driver.switch_to.alert

            alert.accept()

            logging.info("Ventana emergente de confirmación aceptada")
        except:
            logging.info("No se encontró ventana emergente de confirmación")

        time.sleep(2)

    except Exception as e:
        logging.info(e)
        time.sleep(120)
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "//*[@id='top-menu']/div[3]/div[6]/a/div"))).click()

        try:
            alert = driver.switch_to.alert

            alert.accept()

            logging.info("Ventana emergente de confirmación aceptada")
        except:
            logging.info("No se encontró ventana emergente de confirmación")

        time.sleep(2)


def extract() -> DataFrame:
    try:
        file_name_zip = os.listdir(os.path.join(act_dir, 'file'))[0]
        file_path_zip = os.path.join(act_dir, 'file')
        file_name_zip = os.path.join(file_path_zip, file_name_zip)
        with zipfile.ZipFile(file_name_zip, 'r') as zip_ref:

            zip_ref.extractall(file_path_zip)

        os.remove(file_name_zip)
        file_path_zip = os.path.join(act_dir, 'file')
        file_name = os.listdir(os.path.join(act_dir, 'file'))[0]
        file_name = os.path.join(file_path_zip, file_name)
        df = pd.read_csv(file_name, header=0, sep='|')
        print(len(df))
        os.remove(file_name)
        return df
    except Exception as e:
        asyncio.run(enviar_mensaje(f'Error tuya_tipificaciones {e}'))
        logging.info(e)


def transform(df: DataFrame) -> DataFrame:
    try:
        df.columns = df.columns.str.replace(' ', '_')

        # df = df[df['Grabador-Telefónico'].str.contains('sol')]

        df['Fecha_Ultima_Gestion_Telefonica'] = pd.to_datetime(
            df["Fecha_Ultima_Gestion_Telefonica"], errors='coerce')

        df['Fecha_de_Promesa'] = pd.to_datetime(
            df["Fecha_de_Promesa"], errors='coerce')

        df['Fecha_Ultimo_Pago'] = pd.to_datetime(
            df["Fecha_Ultimo_Pago"], errors='coerce')

        df['Dia_de_Vencimiento_de_Cuota'] = pd.to_datetime(
            df["Dia_de_Vencimiento_de_Cuota"], errors='coerce')

        df['Fecha_Traslado_Para_Cobro'] = pd.to_datetime(
            df["Fecha_Traslado_Para_Cobro"], errors='coerce')

        df['Fecha_Corte_Master'] = pd.to_datetime(
            df["Fecha_Corte_Master"], errors='coerce')

        df['Fecha_Ultima_Gestion_Maquina'] = pd.to_datetime(
            df["Fecha_Ultima_Gestion_Maquina"], errors='coerce')

        df['Fecha_Ult._Negociacion'] = pd.to_datetime(
            df["Fecha_Ult._Negociacion"], errors='coerce')

        df['Fecha_Insercion'] = datetime.now()

        logging.info(df.columns)
        print(len(df))

        return df

    except Exception as e:
        asyncio.run(enviar_mensaje(f'Error tuya_tipificaciones {e}'))
        logging.info(e)


def load(df: DataFrame):
    try:
        with engine_61.get_connect() as conn:

            count = pd.read_sql_query(
                text(f"SELECT count(*) FROM bbdd_cs_med_tuya.tb_informe_prejuridico_vigencia"), conn)['count(*)'][0]

            """conn.execute(text(
                f"delete from bbdd_cs_med_tuya.tb_informe_prejuridico_vigencia limit {count}"))"""

            logging.info(f"{count} datos en la tabla preproceso")

            df.to_sql('tb_informe_prejuridico_vigencia', con=conn,
                      if_exists="append", index=False)

            count_ = pd.read_sql_query(
                text(f"SELECT count(*) FROM bbdd_cs_med_tuya.tb_informe_prejuridico_vigencia"), conn)['count(*)'][0]

            logging.info(
                f"{len(df)} datos en dataframe - {count_} datos en tabla post-proceso")

            asyncio.run(enviar_mensaje(
                f'{len(df)} datos cargados \n {count_} datos en la tabla'))
            asyncio.run(enviar_mensaje(
                ("____________________________________")))

    except Exception as e:
        asyncio.run(enviar_mensaje(f'Error tuya_tipificaciones {e}'))
        logging.info(e)
