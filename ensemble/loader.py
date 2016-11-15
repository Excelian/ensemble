#
# (c) 2015, Excelian Ltd
#

import logging
import urllib
import time
import sys

import sqlalchemy
from elasticsearch import helpers
from index_config.consumer_demand import config as consumer_demand_config
import elasticsearch.exceptions

module_name = 'Ensemble.loader'
module_logger = logging.getLogger(module_name)

# Elastic search loggers
logging.getLogger('elasticsearch').setLevel(logging.ERROR)
logging.getLogger('urllib3').setLevel(logging.ERROR)

class Loader(object):
    """ Base Loader class 

    """
    def __init__(self, db_engine, es_conn, max_rows=2000, 
                    seq_field='INSERT_SEQ', sql='', doctype='',
                    chunk_size=100, es_config=None):
        """ Constructor for a sqlloader object
        A loader class for a table an index

        :param db_engine: sqlalchemy engine object
        :param es_conn: Elasticsearch connection object
        :param max_rows: num of rows to retrieve from DB at a time
        :param seq_field: field in DB that corresponds to sequence num
        :param sql: sql query to run to retrieve records
        :param chunk_size: elasticsearch bulk insert chunk size
        """
        self.engine = db_engine
        self.es = es_conn
        self.max_rows = max_rows
        self.seq_field = seq_field
        self.sql = sql
        self.chunk_size = chunk_size
        self.es_config = es_config
        self.index_rollover = es_config['index_rollover']
        
        # Initialise logging
        self.logger = logging.getLogger("%s.%s" % (module_name, 
                                                es_config['template_name']))

        # Create template if it doesn't exist
        self._init_es(es_config)

        # We need to know where we left off by getting the largest
        # sequence number in the index
        self.seq = self._find_last_seq(es_config['all_index'])
        self.logger.info("Last Sequence number = %d" % self.seq)

    def _init_es(self, cfg):
        if not cfg:
            return False
        # Check if templates exists, if not create them
        if not self.es.indices.exists_template(name=cfg['template_name']):
            self.logger.info("Creating template : %s" % cfg['template_name'])
            self.es.indices.put_template(name=cfg['template_name'],
                            body=cfg['template_body'])
        
    def _find_last_seq(self, index_name):
        """ Returns last(maximum) sequence number from elastic search index

        :returns: maximum/last sequence number if index exists. -1 otherwise
        """
        self.logger.info("Finding max seq for index %s" % index_name)
        search_body = {
            "query": { "match_all": {}},
            "size": 1,
            "sort": [{
                "INSERT_SEQ": {"order": "desc"}
            }]
        }
        try:
            res = self.es.search(index=index_name, body=search_body)        
        except elasticsearch.exceptions.NotFoundError:
            self.logger.info('No sequence number found for %s' % index_name)
            return -1
        else:
            return res["hits"]["hits"][0]["sort"][0]
    
    def _runsql(self):
        """ Run the SQL query and return the result set 
            
        :returns: SQLAlchemy result from sql query
        """
        self.logger.info("Running SQL where sequence > %s" % self.seq)
        try:
            results = self.engine.execute(self.sql,
                    (self.max_rows, self.seq)).fetchall()
        except sqlalchemy.exc.ProgrammingError, err:
            self.logger.critical("Error connecting to DB : %s" % err)
            return None
        self.logger.info('Fetched %d rows from DB' % len(results))
        if not len(results):
            self.logger.info("No rows returned from DB. Finished loading")
            return False
        return results

    def _preprocess(self, body):
        """ Use this method to add/modify/aggregate db fields before
            sending this to elasticsearch

        :param body: dict containing sql row result
        :returns: processed dict 
        """
        return body

    def _get_index_name(self, timestamp):
        """
        Return an index name based on the rollover settings
        """
        if self.index_rollover.lower() == 'monthly':
            return "%s-%s" % (self.es_config['all_index'],
                                timestamp.strftime("%m%Y")) 
        elif self.index_rollover.lower() == 'daily':
            return "%s-%s" % (self.es_config['all_index'],
                                timestamp.strftime("%d%m%Y")) 
        else:
            return self.es_config['all_index']

    def _load_elastic(self, sqldata):
        """ iterates through sqldata and bulk loads them into
            elastic search

        :param sqldata: list of sql data rows
        :returns: status of elasticsearch bulk load
        """
        inserts = []
        for r in sqldata:
            body = self._preprocess(dict(r.items()))
            if not body:
                continue # Skip if preprocessing returns False
            index_name = self._get_index_name(body['TIME_STAMP'])
            document = {
                "_index" : index_name,
                "_type" : 'default', # Hardcoded - we only have 1 doctype
                "_id" : body[self.seq_field],
                "_source" : body
                }
            inserts.append(document)

        # update sequence to last item in the results
        self.seq = sqldata[-1][self.seq_field]
        
        # Insert list of documents into elasticsearch
        status = helpers.bulk(self.es, inserts, self.chunk_size)
        self.logger.info("Inserted %d chunks into %s" % (self.chunk_size,
                                                        index_name))
        return status

    def load(self):
        """ Loads all DB rows into Elasticsearch
            
        :returns: status of elasticsearch bulk load
        """
        while True:
            sqldata = self._runsql()
            if not sqldata: # No rows to process. Return for now
                return True

            status = self._load_elastic(sqldata)
            if status[1]:
                self.logger.error("Errors occurred : %s" % status[1])
                # TODO: Should we quit(return False) here 
                # since there are errors ?

            # This should be the remainder and nothing left after that
            # since we didn't exceed max rows
            if len(sqldata) < self.max_rows:
                self.logger.info("Finished inserting up to %d" % self.seq)
                return True

    def __str__(self):
        return "%s loader" % self.es_config['template_name']

class BasicSQLLoader(Loader):
    """ Loader for Generic table 

    """
    def __init__(self, *args, **kwargs):
        super(BasicSQLLoader, self).__init__(*args, **kwargs)

class SessionAttributesLoader(Loader):
    """ Loader for SESSION_ATTRIBUTES Table 

    This is the sampled (default 5 minutes) data that shows all active
    sesssions at the sample point.

    """
    def __init__(self, *args, **kwargs):
        super(SessionAttributesLoader, self).__init__(*args, **kwargs)


class SessionHistoryLoader(Loader):
    """ Loader for SESSION_HISTORY Table 

    This is for the non-sampled SESSION_HISTORY table which contains
    details on ALL sessions.
    
    """
    def __init__(self, *args, **kwargs):
        super(SessionHistoryLoader, self).__init__(*args, **kwargs)


class ResourceMetricsLoader(Loader):
    """ Loader for RESOURCE_METRICS Table 

    We do some preprocessing here to only take attributes that are defined in
    the attr_fields list - others will be discarded.

    """
    attr_fields = { 
            "ncpus" : "NUM_CPUS",
            "nprocs" : "NUM_PROCS",
            "ncores" : "NUM_CORES",
            "nthreads" : "NUM_THREADS",
            "ut" : "CPU_UTILISATION",
            "mem" : "AVAILABLE_MEMORY",
            "io" : "DISK_IO_RATE",
            "swp" : "AVAILABLE_SWAP",
            "pg" : "PAGING_RATE",
            "slot" : "NUM_SLOTS",
            "freeslot" : "NUM_FREE_SLOTS",
            "r15s" : "15_SECOND_LOAD_AVERAGE",
            "r15m" : "15_MINUTE_LOAD_AVERAGE",
            "r1m" : "1_MINUTE_LOAD_AVERAGE",
            "maxmem" : "MAXIMUM_MEMORY",
            "maxswp" : "MAXIMUM_SWAP",
            "ndisks" : "NUM_LOCAL_DISKS",
            "it" : "IDLE_TIME",
    }

    def __init__(self, *args, **kwargs):
        super(ResourceMetricsLoader, self).__init__(*args, **kwargs)

    def _get_attr_val(self, sqlrow):
        # only convert the metrics field and leave the rest as it is
        attr = ResourceMetricsLoader.attr_fields.get(sqlrow['ATTRIBUTE_NAME'],
                sqlrow['ATTRIBUTE_NAME'])
        # CPU utilisation is between 0.0 - 1.0
        # Normalise it to percentage so it is easier to visualise
        if sqlrow['ATTRIBUTE_NAME'] == 'ut' \
                and sqlrow['ATTRIBUTE_VALUE_NUM'] != None:
            val = 100.0 * float(sqlrow['ATTRIBUTE_VALUE_NUM'])
        else:
            val = sqlrow['ATTRIBUTE_VALUE_NUM']
        return (attr, val)

    def _load_elastic(self, sqldata):
        """ iterates through sqldata and bulk loads them into
            elastic search

            Since each sql row contains only a single attribute-value pair
            we will need to finish processing all the records before inserting
            them into ES as the mapping contains all attributes for a given
            resource-timestamp

        :param sqldata: list of sql data rows
        :returns: status of elasticsearch bulk load
        """
        from collections import defaultdict
        attributes = ResourceMetricsLoader.attr_fields.keys()
        records = defaultdict(lambda: defaultdict(int))
        for sd in sqldata:
            r = dict(sd.items())
            if r['ATTRIBUTE_NAME'] not in attributes:
                continue
            # Only store hostnames and not FQDN for resources
            r['RESOURCE_NAME'] = r['RESOURCE_NAME'].split('.')[0]

            (attr, val) = self._get_attr_val(r)
            records[r.get('RESOURCE_NAME'),r.get('TIME_STAMP')][attr] = val
            records[r.get('RESOURCE_NAME'),r.get('TIME_STAMP')]['INSERT_SEQ'] = r['INSERT_SEQ']

        # Construct docs from records
        inserts = [] 
        for k, v in records.iteritems():
            body = { attr: val for attr, val in v.iteritems() } 
            body['RESOURCE_NAME'], body['TIME_STAMP'] = k
            document = {
                "_index" : self._get_index_name(body['TIME_STAMP']),
                "_type" : 'default',
                "_source" : body
            }
            inserts.append(document)
        
        # Insert list of documents into elasticsearch
        self.logger.info("Loading chunk into elasticsearch")
        status = helpers.bulk(self.es,
                                inserts,
                                self.chunk_size)
        self.logger.info("Finished loading chunk into elasticsearch")

        # update sequence to last item in the results
        #self.seq = dict(results[-1].items())[self.id_field]
        self.seq = sqldata[-1][self.seq_field]
        
        return status


class ConsumerResourceAllocationLoader(Loader):
    """ Loader for the CONSUMER_RESOURCE_ALLOCATION table

    """
    def __init__(self, *args, **kwargs):
        super(ConsumerResourceAllocationLoader, self).__init__(*args,
                **kwargs)

class ConsumerDemandLoader(Loader):
    """ Loader for the CONSUMER_DEMAND Table

    """
    def __init__(self, *args, **kwargs):
        super(ConsumerDemandLoader, self).__init__(*args,
                **kwargs)
