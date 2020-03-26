#!/usr/bin/env python
from dotenv import load_dotenv
import logging
import os

load_dotenv()

env = os.getenv

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

WORKDIR = os.path.dirname(os.path.abspath(__file__))
APPPATH = os.path.join(WORKDIR, "..")
LOGPATH = os.path.join(APPPATH, "LOG")
LOGFILE = os.path.join(LOGPATH, f"LOG_.log")

logging.basicConfig(filename=LOGFILE, format='%(asctime)s - %(message)s', level=logging.DEBUG)

def logerror(err):
    logging.error(err)

def logwarning(warn):
    logging.warning(warn)

def loginfo(info):
    logging.info(info)

def logexception(err):
    logging.exception(err)   
