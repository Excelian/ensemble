#
# (c) 2015, Excelian Ltd
#
# file: ensemble/server.py
# description : server ETL process  
# ============================================================================

import sys
import logging
import sched
import time
import yaml
import os

from loader import (
        BasicSQLLoader,
        ConsumerDemandLoader
#        ConsumerResourceAllocationLoader,
#        ResourceMetricsLoader,
#        SessionHistoryLoader,
#        SessionAttributesLoader
)
from helpers import get_db_engine, get_es_conn
import index_config.consumer_demand

CONFIG_FILE="config.yml" # always look for config.yml in CWD

def run(loaders, interval):
    """ Main loop of server ETL that continuously schedules each loader

    :param loaders: list of loader objects 
    :param interval: duration in seconds between each loading iteration
    """ 
    while True:
        logging.info('Begin scheduling tasks')
        scheduler = sched.scheduler(time.time, time.sleep)
        
        # Add tasks to scheduler
        for loader in loaders:
            loader.load()
            scheduler.enter(1, 1, loader.load, ())

        # Run all tasks after adding them
        scheduler.run()
        logging.info('All tasks completed, sleep %d seconds' % interval)
        time.sleep(interval)  # Sleep before next scheduling interval
    return

def get_loaders(cfg, engine, es):
    loaders = [] 
    for loadername, loaderconf in cfg['loaders'].iteritems():
        logging.info("Creating loader : %s" % loadername)
        if loadername == "resources":
            loaders.append(ResourceMetricsLoader(db_engine=engine,
                            es_conn=es,
                            index=loaderconf['index'],
                            sql=loaderconf['sql'],
                            doctype=loaderconf['type']))
        elif loadername == "consumer_demand":
            loaders.append(ConsumerDemandLoader(db_engine=engine,
                            es_conn=es,
                            index=loaderconf['index'],
                            sql=loaderconf['sql'],
                            doctype=loaderconf['type']))
        elif loadername == "consumer_resource_allocation":
            loaders.append(ConsumerResourceAllocationLoader(db_engine=engine,
                            es_conn=es,
                            index=loaderconf['index'],
                            sql=loaderconf['sql'],
                            doctype=loaderconf['type']))
        elif loadername == "session_attributes":
            loaders.append(SessionAttributesLoader(db_engine=engine,
                            es_conn=es,
                            index=loaderconf['index'],
                            sql=loaderconf['sql'],
                            doctype=loaderconf['type']))
        elif loadername == "session_history":
            loaders.append(SessionHistoryLoader(db_engine=engine,
                            es_conn=es,
                            index=loaderconf['index'],
                            sql=loaderconf['sql'],
                            doctype=loaderconf['type']))
        else:
            loaders.append(BasicSQLLoader(db_engine=engine,
                            es_conn=es,
                            index=loaderconf['index'],
                            sql=loaderconf['sql'],
                            doctype=loaderconf['type']))
    return loaders

def main():
    cfg = sys.argv[1] if len(sys.argv) > 1 else CONFIG_FILE
    if not os.path.isfile(cfg):
        sys.exit("%s does not exist or is not valid" % cfg)
    with open(cfg, 'r') as ymlfile:
        cfg = yaml.load(ymlfile)
    setup = cfg['setup']

    # Set up logging
    logger = logging.getLogger("Ensemble")
    logger.setLevel(logging.INFO)
    sh = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s-[%(levelname)s]'
                                    '-[%(name)s]- %(message)s')
    sh.setFormatter(formatter)
    logger.addHandler(sh)
    logger.info("Starting up with settings in %s" % CONFIG_FILE)

    # Get a DB connection
    engine = get_db_engine(setup['db_host'], setup['db_port'],
            setup['db_name'], setup['db_user'], setup['db_pass'])
    if not engine:
        sys.exit("Invalid configuration for DB")

    logger.info("DB Engine done")
    # Get an Elasticsearch connection
    es_hosts = setup['es_hosts'].split(',')
    for _ in range(setup['es_max_retries']):
        es = get_es_conn(es_hosts, setup['es_ssl'],
                    setup['es_verify_certs'], setup['es_cacerts']) 
        if es:
            logger.info("Connected to Elasticsearch : %s" \
                % es.info().get('cluster_name', 'unknown'))
            break

        logger.warning("Retrying connection to Elasticsearch")
        time.sleep(setup['es_retry_wait'])
    else:
        logger.criticial("Failed Connecting to Elasticsearch")
        sys.exit(1)

    # Build our list of SQL loaders
    logger.info("Building list of loaders")
    #loaders = get_loaders(cfg, engine, es) 

    # We got all our configs, let's run now
    logger.info("Initialisation complete: let's do some ETL !")
    loaderconf = cfg['loaders'].get('consumer_demand', {})
    c = ConsumerDemandLoader(db_engine=engine,
                    es_conn=es,
                    sql=loaderconf['sql'],
                    es_config=index_config.consumer_demand.config)
    c.load()
    sys.exit(1)

if __name__ == '__main__':
    main()
