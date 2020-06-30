import unittest
import pymysql
import mysql_dao
from mysql_dao import TaskDBMySqlDao
import json
from task_status import TaskStatus
import uuid
import time
from init_mysql_connector_cursor import init_mysql_connector_and_cursor
import platform
import requests

def what_env():
    # Local mac book
    if platform.system() != 'Linux':
        return 'Local'
    
    # Mongo DB node on AWS
    if requests.get('http://169.254.169.254/latest/meta-data/public-ipv4') == '34.211.21.127':
        return 'Mongo'

    # MySQL node on AWS
    if requests.get('http://169.254.169.254/latest/meta-data/public-ipv4') == '54.70.196.132':
        return 'MySQL'

    return None