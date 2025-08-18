import os
import mysql.connector
from mysql.connector import pooling
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = dict(
    host=os.getenv("DB_HOST", "localhost"),
    port=int(os.getenv("DB_PORT", "3306")),
    user=os.getenv("DB_USER", "root"),
    password=os.getenv("DB_PASSWORD", "1234"),
    database=os.getenv("DB_NAME", "buscador_normativo"),
)

POOL_NAME = "mysql_pool"
POOL_SIZE = 5

_connection_pool = pooling.MySQLConnectionPool(pool_name=POOL_NAME, pool_size=POOL_SIZE, **DB_CONFIG)

def get_conn():
    return _connection_pool.get_connection()
