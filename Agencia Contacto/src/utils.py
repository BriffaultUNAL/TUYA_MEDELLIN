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
from datetime import datetime, timedelta
import datetime as dt
import calendar
from unidecode import unidecode


act_dir = os.path.dirname(os.path.abspath(__file__))
proyect_dir = os.path.join(act_dir, '..')
sys.path.append(proyect_dir)

path_to_edgedriver = os.path.join(
    proyect_dir, 'edgedriver', 'msedgedriver.exe')
log_dir = os.path.join(proyect_dir, 'log', 'logs_main.log')
yml_credentials_dir = os.path.join(proyect_dir, 'config', 'credentials.yml')

today = datetime.now()

current_year = today.year
current_month = today.month


first_day_week = today - dt.timedelta(days=today.weekday())

first_day_week = first_day_week.day
today = today.day


print(first_day_week)

last_day = calendar.monthrange(current_year, current_month)[1]

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

        asyncio.run(enviar_mensaje(f'Tuya Agencia contacto'))

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

        WebDriverWait(driver, 60).until(
            EC.presence_of_element_located((By.XPATH, "//*[@id='dialog']/div[3]/table/tbody/tr[6]/td[2]/a"))).click()

        driver.get(
            """https://172.226.124.46/vsmart/index.php?rtr=informes&ctr=informeSeguimiento&titulocriterio=Resumen%20de%20Seguimiento%20por%20Agencia&acc=""")

        WebDriverWait(driver, 60).until(
            EC.presence_of_element_located((By.CLASS_NAME, "select2-chosen"))).click()

        WebDriverWait(driver, 60).until(
            EC.presence_of_element_located((By.XPATH, "//*[@id='select2-drop']/ul/li[20]/div"))).click()

        WebDriverWait(driver, 60).until(
            EC.presence_of_element_located((By.ID, "s2id_col"))).click()

        WebDriverWait(driver, 60).until(
            EC.presence_of_element_located((By.XPATH, "//*[@id='select2-drop']/ul/li[54]/div"))).click()

        WebDriverWait(driver, 60).until(
            EC.presence_of_element_located((By.XPATH, "//*[@id='s2id_ope']/a/span[1]"))).click()

        WebDriverWait(driver, 60).until(
            EC.presence_of_element_located((By.XPATH, "//*[@id='select2-drop']/ul/li[13]"))).click()

        if int(today) < int(first_day_week):

            WebDriverWait(driver, 60).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "a#btnCalendario .ui-datepicker-trigger"))).click()

            WebDriverWait(driver, 60).until(
                EC.presence_of_element_located((By.XPATH, "//*[@id='ui-datepicker-div']/div[1]/a[1]/span"))).click()

            calendario = WebDriverWait(driver, 60).until(
                EC.presence_of_element_located((By.XPATH, "//*[@id='ui-datepicker-div']/table/tbody")))

            days = calendario.find_elements(By.TAG_NAME, "a")

            for day in days:
                if day.text == f'{first_day_week}':
                    day.click()

            WebDriverWait(driver, 60).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "a#btnCalendario .ui-datepicker-trigger"))).click()

            WebDriverWait(driver, 60).until(
                EC.presence_of_element_located((By.XPATH, "//*[@id='ui-datepicker-div']/div[1]/a[2]/span"))).click()

            calendario = WebDriverWait(driver, 60).until(
                EC.presence_of_element_located((By.XPATH, "//*[@id='ui-datepicker-div']/table/tbody")))

            days = calendario.find_elements(By.TAG_NAME, "a")

            for day in days:
                if day.text == f'{today}':
                    day.click()

            """element = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//*[@id='ui-datepicker-div']/div[2]/button")))

            action_chains.double_click(element).perform()"""

        else:

            WebDriverWait(driver, 60).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "a#btnCalendario .ui-datepicker-trigger"))).click()

            calendario = WebDriverWait(driver, 60).until(
                EC.presence_of_element_located((By.XPATH, "//*[@id='ui-datepicker-div']/table/tbody")))

            days = calendario.find_elements(By.TAG_NAME, "a")

            for day in days:
                if day.text == f'{first_day_week}':
                    day.click()

            WebDriverWait(driver, 60).until(
                EC.presence_of_element_located((By.XPATH, "//*[@id='ui-datepicker-div']/div[2]/button[2]"))).click()

            WebDriverWait(driver, 60).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "a#btnCalendario .ui-datepicker-trigger"))).click()

            calendario = WebDriverWait(driver, 60).until(
                EC.presence_of_element_located((By.XPATH, "//*[@id='ui-datepicker-div']/table/tbody")))

            days = calendario.find_elements(By.TAG_NAME, "a")

            for day in days:
                if day.text == f'{today}':
                    day.click()

            """element = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//*[@id='ui-datepicker-div']/div[2]/button[2]")))

            action_chains.double_click(element).perform()"""

        time.sleep(2)
        try:
            WebDriverWait(driver, 60).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "a#btnAgregarCriterio .ui-icon-plus"))).click()
        except Exception as e:
            print(e)

        WebDriverWait(driver, 60).until(
            EC.presence_of_element_located((By.ID, "add_tab"))).click()

        WebDriverWait(driver, 60).until(
            EC.presence_of_element_located((By.XPATH, "/html/body/div[2]/div[1]/div[1]/div[3]/div[2]/div[3]/div[2]/table/tbody/tr[4]/td[1]/input"))).click()

        WebDriverWait(driver, 60).until(
            EC.presence_of_element_located((By.CLASS_NAME, "select2-container"))).click()

        WebDriverWait(driver, 60).until(
            EC.presence_of_element_located((By.XPATH, "//*[@id='select2-drop']/ul/li[56]/div"))).click()
        time.sleep(10)
        WebDriverWait(driver, 60).until(
            EC.presence_of_element_located((By.ID, "add_tab"))).click()

        WebDriverWait(driver, 120).until(
            EC.presence_of_element_located((By.XPATH, "/html/body/div[2]/div[1]/div[1]/div[3]/div[2]/div[4]/div/div[2]/a[3]"))).click()

        WebDriverWait(driver, 120).until(
            EC.presence_of_element_located((By.XPATH, "/html/body/div[2]/div[1]/div[1]/div[3]/div[2]/div[4]/div/div/span[1]/a[1]"))).click()

        WebDriverWait(driver, 120).until(
            EC.presence_of_element_located((By.XPATH, "/html/body/div[2]/div[1]/div[1]/div[3]/div[2]/div[4]/div/div/div[3]/form/div[4]/input[1]"))).click()

        ###############################
        time.sleep(30)

        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "//*[@id='top-menu']/div[3]/div[6]/a/div"))).click()

        try:
            alert = driver.switch_to.alert

            alert.accept()

            logging.info("Ventana emergente de confirmación aceptada")
        except:
            logging.info("No se encontró ventana emergente de confirmación")

    except Exception as e:
        logging.info(e)
        time.sleep(30)
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "//*[@id='top-menu']/div[3]/div[6]/a/div"))).click()

        try:
            alert = driver.switch_to.alert

            alert.accept()

            logging.info("Ventana emergente de confirmación aceptada")
        except:
            logging.info("No se encontró ventana emergente de confirmación")


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
        df = pd.read_excel(file_name, header=3)
        os.remove(file_name)
        return df
    except Exception as e:
        asyncio.run(enviar_mensaje(f'Error tuya_contacto {e}'))
        logging.info(e)


def transform(df: DataFrame) -> DataFrame:
    try:
        df.columns = df.columns.str.replace(' ', '_')

        # df = df[df['Grabador'].str.contains('sol')]
        df.columns = [unidecode(x) for x in df.columns if isinstance(x, str)]
        df['Fecha_Gestion'] = pd.to_datetime(df["Fecha_Gestion"])
        df['Fecha_Gestion_Real'] = pd.to_datetime(df["Fecha_Gestion_Real"])
        logging.info(df.columns)
        logging.info(len(df))

        return df

    except Exception as e:
        asyncio.run(enviar_mensaje(f'Error tuya_contacto {e}'))
        logging.info(e)


def load(df: DataFrame):
    try:
        with engine_61.get_connect() as conn:

            count = pd.read_sql_query(
                text(f"SELECT count(*) FROM bbdd_cs_med_tuya.tb_seguimiento_agencia_contacto"), conn)['count(*)'][0]

            conn.execute(text(
                f"delete from bbdd_cs_med_tuya.tb_seguimiento_agencia_contacto limit {count}"))

            logging.info(f"{count} datos en la tabla preproceso")

            df.to_sql('tb_seguimiento_agencia_contacto', con=conn,
                      if_exists="append", index=False)

            count_ = pd.read_sql_query(
                text(f"SELECT count(*) FROM bbdd_cs_med_tuya.tb_seguimiento_agencia_contacto"), conn)['count(*)'][0]

            logging.info(
                f"{len(df)} datos en dataframe - {count_} datos en tabla post-proceso")

            logging.info(first_day_week)

            asyncio.run(enviar_mensaje(
                f'first day :{first_day_week} \n {len(df)} datos cargados \n {count_} datos en la tabla'))

            asyncio.run(enviar_mensaje(
                ("____________________________________")))

    except Exception as e:
        asyncio.run(enviar_mensaje(f'Error tuya_canales_autorizados {e}'))
        logging.info(e)
