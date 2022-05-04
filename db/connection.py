import json
from pathlib import Path
from sqlalchemy import create_engine

PATH = Path(__file__).resolve().parent

with open(f"{PATH}/db_config.json", 'r', encoding='utf-8') as f:
    data = f.read()
    DATABASES = json.loads(data)


def get_tidb_engine():
    address = f"mysql+pymysql://{DATABASES['tidb']['user']}:{DATABASES['tidb']['password']}@" \
              f"{DATABASES['tidb']['host']}:{DATABASES['tidb']['port']}/{DATABASES['tidb']['db_name']}"
    return create_engine(address, isolation_level='READ COMMITTED')


def get_timeweb_engine():
    address = f"mysql+pymysql://{DATABASES['timeweb']['user']}:{DATABASES['timeweb']['password']}@" \
              f"{DATABASES['timeweb']['host']}:{DATABASES['timeweb']['port']}/{DATABASES['timeweb']['db_name']}"
    return create_engine(address, isolation_level='READ COMMITTED')


def get_risk_score_engine():
    address = f"postgresql+pg8000://{DATABASES['risk_score']['user']}:{DATABASES['risk_score']['password']}@" \
              f"{DATABASES['risk_score']['host']}:{DATABASES['risk_score']['port']}/{DATABASES['risk_score']['db_name']}"
    return create_engine(address, isolation_level='READ COMMITTED')


def get_tip3_engine():
    address = f"postgresql+pg8000://{DATABASES['tip3']['user']}:{DATABASES['tip3']['password']}@" \
              f"{DATABASES['tip3']['host']}:{DATABASES['tip3']['port']}/{DATABASES['tip3']['db_name']}"
    return create_engine(address, isolation_level='READ COMMITTED')


def get_result_db_engine():
    address = f"postgresql+pg8000://{DATABASES['result']['user']}:{DATABASES['result']['password']}@" \
              f"{DATABASES['result']['host']}:{DATABASES['result']['port']}/{DATABASES['result']['db_name']}"
    return create_engine(address, isolation_level='READ COMMITTED')
