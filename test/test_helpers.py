import string
import random
import logging
import sys
import sqlalchemy
import unittest2 as unittest
from ensemble.helpers import get_db_engine, get_es_conn

class Helper_test(unittest.TestCase):
    def setUp(self):
        logger = logging.getLogger('Test')
        logger.levl = logging.DEBUG
        stream_handler = logging.StreamHandler(sys.stdout)
        logger.addHandler(stream_handler)

    def test_get_db_engine_none_args(self):
        self.assertFalse(get_db_engine(None, 8080, "test_DB", "user", 
                        "pass", "Test"))

    def test_get_db_engine_returns_sqlalchemy(self):
        eng = get_db_engine("host", 8080, "test_DB", "user", "pass", "Test")
        self.assertIsInstance(eng, sqlalchemy.engine.Engine)

    def test_get_es_conn_empty_hostlist(self):
        self.assertFalse(get_es_conn(es_hostlist=[], logger_name="Test"))
