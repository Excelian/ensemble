import logging
import urllib
import sqlalchemy
from elasticsearch import Elasticsearch, TransportError, ConnectionError
   
def get_db_engine(host, port, db_name, user, passwd):
    """ Get sqlalchemy engine from setup config

    :param host: hostname of DB server
    :param port: port number of DB
    :param db_name: DB name
    :param user: DB user name
    :param passwd: DB password 
    :param logger_name: optional name of logger
    :returns: sqlalchemy engine object. Returns False if config is missing.

    """
    logger = logging.getLogger("Ensemble")
    try:
        if not all([host, port, db_name, user, passwd]):
            logger.error("One or more DB config empty")
            return False
    except KeyError, err:
        logger.error("One or more DB config not specified")
        return False

    c = "Driver=FreeTDS;SERVER=%s;DATABASE=%s;UID=%s;PWD=%s;port=%s;" \
        "TDS_Version=8.0" % (host, db_name, user, passwd, port)
    logger.info("Connection string to db = %s" % c)
    return sqlalchemy.create_engine(
            'mssql+pyodbc:///?odbc_connect=%s' % (urllib.quote_plus(c)))

def get_es_conn(es_hostlist=None, es_user=None, es_pass=None, ssl=False,
        verify_certs=False, cacerts_path=None):
    """ Returns an elasticsearch obj using config from setup

    :param es_hostlist: list of node hostnames in Elasticsearch cluster
    :param ssl: Enable SSL (defaults to False) 
    :param verify_certs:  verify SSL certs
    :param cacerts_path: path to CA certificates
    :param logger_name: optional name of logger
    :returns: Elasticsearch object 

    """
    logger = logging.getLogger("Ensemble")
    if not es_hostlist:
        logger.error("No valid hosts for Elasticsearch")
        return False
    assert(type(es_hostlist) == list)

    try:
        if ssl:
            es = Elasticsearch(
                    es_hostlist,
                    http_auth=(es_user, es_pass),
                    use_ssl=True,
                    verify_certs=verify_certs,
                    cacerts=cacerts_path)
        else:
            es = Elasticsearch(
                    es_hostlist,
                    http_auth=(es_user, es_pass),
                    # sniff before doing anything
                    sniff_on_start=True,
                    # refresh nodes after a node fails to respond
                    sniff_on_connection_fail=True,
                    # and also every 60 seconds
                    sniffer_timeout=60)
    except TransportError, ConnectionError:
        return False


    try:
        esinfo = es.info()
    except:
        logger.exception("Unable to connect to Elasticsearch Cluster")
        return False
    else:
        logger.info("Connected to ES Node : %s" % esinfo['name'])
    return es

