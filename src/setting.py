#!/usr/bin/env python
from dotenv import load_dotenv
import logging
import os

load_dotenv()

MSSQL_HOST = os.getenv("MSSQL_HOST")
MSSQL_USER = os.getenv("MSSQL_USER")
MSSQL_PWD  = os.getenv("MSSQL_PWD")
MSSQL_DB   = os.getenv("MSSQL_DB")
MSSQL_PORT = os.getenv("MSSQL_PORT")

MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PWD  = os.getenv("MYSQL_PWD")
MYSQL_DB   = os.getenv("MYSQL_DB")
MYSQL_PORT = os.getenv("MYSQL_PORT")

work_dir = os.path.dirname(os.path.abspath(__file__))

if os.name == "posix":
    logging.basicConfig(filename=work_dir+"/../log/error.log", format='%(asctime)s - %(message)s', level=logging.ERROR)
else:
    logging.basicConfig(filename=work_dir+"/error.log", format='%(asctime)s - %(message)s', level=logging.ERROR)

def log_error(message):
    logging.exception(message)
    print message
