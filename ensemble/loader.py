#
# (c) 2015, Excelian Ltd
#
# file: ensemble/sqlloader.py
# description : sqlloader module
# ============================================================================

import logging
import urllib
import time
import sys

import sqlalchemy
from elasticsearch import helpers
from index_config.consumer_demand import config as consumer_demand_config
import elasticsearch.exceptions

module_name = 'Ensemble.sqlloader'
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
            self.logger("Creating template : %s" % cfg['template_name'])
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
        #res = self.engine.execute(self.sql, (self.max_rows, self.seq))
        #results = res.fetchall()
        results = self.engine.execute(self.sql,
                (self.max_rows, self.seq)).fetchall()
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

    def _create_mapping(self):
        """ Create custom mapping for sessions doctype 
        """
        self.es.indices.put_mapping(
                index=self.index,
                doc_type=self.doctype,
                body={
                    self.doctype: {
                        "properties": {
                            "CLUSTER_NAME": {
                                "type": "string",
                                "index": "not_analyzed",
                            },
                            "TIME_STAMP": {
                                "type": "date",
                                "format": "dateOptionalTime"
                            },
                            "SESSION_ID": {"type": "long"},
                            "SESSION_NAME": {
                                "type": "string",
                                "index": "not_analyzed",
                            },
                            "APP_NAME": {
                                "type": "string",
                                "index": "not_analyzed",
                            },
                            "CONSUMER_NAME": {
                                "type": "string",
                                "index": "not_analyzed",
                            },
                            "SUBMISSION_USER": {
                                "type": "string",
                                "index": "not_analyzed",
                            },
                            "PRIORITY": {"type": "integer"},
                            "COMMON_DATA_SIZE": {"type": "long"},
                            "NUM_TASK_PENDING": {"type": "integer"},
                            "NUM_TASK_RUNNING": {"type": "integer"},
                            "NUM_TASK_DONE": {"type": "integer"},
                            "NUM_TASK_ERROR": {"type": "integer"},
                            "NUM_TASK_CANCELED": {"type": "integer"},
                            "TOTAL_INPUT_SIZE": {"type": "long"},
                            "TOTAL_OUTPUT_SIZE": {"type": "long"},
                            "TOTAL_TASKS_RUNTIME": {"type": "long"},
                            "TOTAL_UNSUCCESS_TASKRUNS": {
                                "type": "integer"
                            },
                            "TOTAL_UNSUCCESS_TASKSRUNTIME": {
                                "type": "long"
                            },
                            "INSERT_SEQ": {"type": "long"},
                        }
                    }
                }
        )

class SessionHistoryLoader(Loader):
    """ Loader for SESSION_HISTORY Table 

    This is for the non-sampled SESSION_HISTORY table which contains
    details on ALL sessions.
    
    """
    def __init__(self, *args, **kwargs):
        super(SessionHistoryLoader, self).__init__(*args, **kwargs)

    def _create_mapping(self):
        """ Create custom mapping for Resource doctype 
        """
        self.es.indices.put_mapping(
                index=self.index,
                doc_type=self.doctype,
                body={
                    self.doctype: {
                        "properties": {
                            "ABORT_SESSION_IF_TASK_FAIL": {"type": "boolean"},
                            "APP_NAME": {
                                "type": "string",
                                "index": "not_analyzed"
                            },
                            "CLIENT_HOST_NAME": {"type": "string",},
                            "CLIENT_IP_ADDRESS": {"type": "string"},
                            "CLIENT_OS_USER_NAME": {
                                "type": "string",
                                "index": "not_analyzed"
                            },
                            "CLIENT_VERSION": {
                                "type": "string",
                                "index": "not_analyzed"
                            },
                            "CLUSTER_NAME": {
                                "type": "string",
                                "index": "not_analyzed"
                            },
                            "COMMON_DATA_COMPRESSED": {"type": "boolean"},
                            "COMMON_DATA_SIZE": {"type": "long"},
                            "COMPRESSION_ENABLED": {"type": "boolean"},
                            "COMPRESSION_THRESHOLD": {"type": "long"},
                            "CONSUMER_NAME": {"type": "string"},
                            "CREATE_TIME": {
                                "type": "date",
                                "format": "dateOptionalTime"
                            },
                            "DIRECT_DATA_TRANSFER_ENABLED": {
                                "type": "boolean"
                            },
                            "END_TIME": {
                                "type": "date",
                                "format": "dateOptionalTime"
                            },
                            "INSERT_SEQ": {
                                "type": "long"
                            },
                            "LAST_ERROR_TIME": {
                                "type": "date",
                                "format": "dateOptionalTime"
                            },
                            "NUM_COMMON_DATA_UPDATES": {"type": "long"},
                            "NUM_TASK_CANCELED": {"type": "integer"},
                            "NUM_TASK_DONE": {"type": "integer"},
                            "NUM_TASK_ERROR": {"type": "integer"},
                            "NUM_TASK_PENDING": {"type": "integer"},
                            "NUM_TASK_RESUMING": {"type": "integer"},
                            "NUM_TASK_RUNNING": {"type": "integer"},
                            "NUM_TASK_YIELDED": {"type": "integer"},
                            "PARENT_ID": {
                                "type": "string",
                                "index": "not_analyzed"
                            },
                            "POLICY_TAG_PATH": {
                                "type": "string",
                                "index": "not_analyzed"
                            },
                            "PREEMPTION_RANK": {"type": "long"},
                            "PREEMPTIVE": {"type": "boolean"},
                            "PREEMPT_TIME":{"type": "long"},
                            "PRIORITY": {"type": "integer"},
                            "PROTOCOL_VERSION": {"type": "integer"},
                            "RAW_COMMON_DATA_SIZE": {"type": "long"},
                            "REASON": { "type": "string"},
                            "RECLAIM_TIME": { "type": "long"},
                            "RECOVERABLE": { "type": "boolean"},
                            "RESOURCE_GROUP_FILTER": { "type": "string"},
                            "ROOT_SESSION_ID": { "type": "long"},
                            "SERVICE": {"type": "string"},
                            "SERVICE_INSTANCES": {"type":"integer"},
                            "SERVICE_TO_SLOT_RATIO": {"type":"string"},
                            "SESSION_BINDING_FAILURES": {"type":"string"},
                            "SESSION_ID":{"type":"long"},
                            "SESSION_METADATA": {"type":"string"},
                            "SESSION_NAME": {"type":"string"},
                            "SESSION_STATE": {"type":"string"},
                            "SESSION_TAG": {"type":"string"},
                            "SESSION_TIMEOUT": {"type": "long"},
                            "SESSION_TYPE": {"type": "string"},
                            "SESSION_UPDATE_FAILURES": {"type": "string"},
                            "SUBMISSION_USER": {
                                "type": "string",
                                "index": "not_analyzed"
                            },
                            "SYMPHONY_VERSION": {
                                "type": "string",
                                "index": "not_analyzed"
                            },
                            "TASKS_INTERRUPTED_BY_PREEMPT": {
                                "type":"integer"
                            },
                            "TASKS_INTERRUPTED_BY_RECLAIM": {
                                "type": "integer"
                            },
                            "TASK_DISPATCH_ORDER": {
                                "type": "string",
                                "index": "not_analyzed"
                            },
                            "TASK_EXECUTION_TIMEOUT":{ "type": "integer"},
                            "TASK_RETRY_LIMIT": { "type": "long"},
                            "TIME_STAMP": {
                                "type": "date",
                                "format": "dateOptionalTime"
                            },
                            "TIME_STAMP_GMT": {"type": "long"},
                            "TOTAL_COMMON_DATA_UPDATE_SIZE": {
                                "type": "long"
                            },
                            "TOTAL_INPUT_SIZE": {
                                "type": "long"
                            },
                            "TOTAL_OUTPUT_SIZE": {
                                "type": "long"
                            },
                            "TOTAL_TASKS_RUNTIME": {
                                "type": "double"
                            },
                            "TOTAL_TASKS_SUBMIT2START_TIME": {
                                "type": "double"
                            },
                            "TOTAL_UNSUCCESS_TASKRUNS": { "type": "integer"},
                            "TOTAL_UNSUCCESS_TASKSRUNTIME": {
                                "type": "double"
                            }
                        }
                    }
                }
        )

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
        records = defaultdict(lambda: defaultdict(int))
        attributes = ResourceMetricsLoader.attr_fields.keys()
        for sd in sqldata:
            r = dict(sd.items())
            if r['ATTRIBUTE_NAME'] not in attributes:
                continue
            # Only store hostnames and not FQDN for resources
            r['RESOURCE_NAME'] = r['RESOURCE_NAME'].split('.')[0]

            (attr, val) = self._get_attr_val(r)
            self.logger.debug("resource-time does not exist")
            records[r.get('RESOURCE_NAME'),r.get('TIME_STAMP')][attr] = val
            records[r.get('RESOURCE_NAME'),r.get('TIME_STAMP')]['INSERT_SEQ'] = r['INSERT_SEQ']

        # Construct docs from records
        inserts = list()
        for k, v in records.iteritems():
            body = { attr: val for attr, val in v.iteritems() } 
            body['RESOURCE_NAME'], body['TIME_STAMP'] = k
            document = {
                "_index" : self.index,
                "_type" : self.doctype,
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

    def _create_mapping(self):
        """ Create custom mapping for Resource doctype 
        """
        self.es.indices.put_mapping(
                index=self.index,
                doc_type=self.doctype,
                body={
                    self.doctype: {
                        "properties": {
                            "CLUSTER_NAME": {
                                "type": "string",
                                "index": "not_analyzed",
                            },
                            "TIME_STAMP": {
                                "type": "date",
                                "format": "dateOptionalTime"
                            },
                            "RESOURCE_NAME": {
                                "type": "string",
                                "index": "not_analyzed",
                            },
                            "RESOURCE_TYPE": {
                                "type": "string",
                                "index": "not_analyzed",
                            },
                            "NUM_CPUS": {"type": "integer"},
                            "NUM_PROCS": {"type": "integer"},
                            "NUM_CORES": {"type": "integer"},
                            "NUM_THREADS": {"type": "integer"},
                            "CPU_UTILISATION": {"type": "long"},
                            "AVAILABLE_MEMORY": {"type": "long"},
                            "AVAILABLE_SWAP": {"type": "long"},
                            "PAGING_RATE": {"type": "long"},
                            "DISK_IO_RATE": {"type": "long"},
                            "NUM_SLOTS": {"type": "integer"},
                            "NUM_FREE_SLOTS": {"type": "integer"},
                            "15_SECOND_LOAD_AVERAGE": {"type": "long"},
                            "15_MINUTE_LOAD_AVERAGE": {"type": "long"},
                            "1_MINUTE_LOAD_AVERAGE": {"type": "long"},
                            "MAXIMUM_MEMORY": {"type": "long"},
                            "MAXIMUM_SWAP": {"type": "long"},
                            "NUM_LOCAL_DISKS": {"type": "integer"},
                            "IDLE_TIME": {"type": "long"},
                            "INSERT_SEQ": {"type": "long"},
                        }
                    }
                }
        )

class ConsumerResourceAllocationLoader(Loader):
    """ Loader for the CONSUMER_RESOURCE_ALLOCATION table

    """
    def __init__(self, *args, **kwargs):
        super(ConsumerResourceAllocationLoader, self).__init__(*args,
                **kwargs)

    def _create_mapping(self):
        """ Create custom mapping for Consumer index
        """
        self.es.indices.put_mapping(
                index=self.index,
                doc_type=self.doctype,
                body={
                    self.doctype: {
                        "properties": {
                            "CONSUMER_NAME": {
                                "type": "string",
                                "index": "not_analyzed",
                            },
                            "INSERT_SEQ": {"type": "long"},
                            "RESOURCE_GROUP": {
                                "type": "string",
                                "index": "not_analyzed",
                            },
                            "CLUSTER_NAME": {
                                "type": "string",
                                "index": "not_analyzed",
                            },
                            "TIME_STAMP": {
                                "type": "date",
                                "format": "dateOptionalTime"
                            },
                            "ALLOCATED_SHARE": {
                                "type": "integer",
                                "null_value": 0
                            },
                            "ALLOCATED_OWN": {
                                "type": "integer",
                                "null_value": 0
                            },
                            "ALLOCATED_BORROW": {
                                "type": "integer",
                                "null_value": 0
                            },
                            "ALLOCATED_LEND": {
                                "type": "integer",
                                "null_value": 0
                            },
                            "BORROW_FROM": {
                                "type": "string",
                                "index": "not_analyzed",
                            },
                            "BORROW_LIMIT": {
                                "type": "integer",
                                "null_value": 0
                            },
                            "LENDING_LIMIT": {
                                "type": "integer",
                                "null_value": 0
                            },
                            "PLANNED_OWN": {
                                "type": "integer",
                                "null_value": 0
                            },
                            "USED": {
                                "type": "integer",
                                "null_value": 0
                            },
                            "LEND_TO": {
                                "type": "string",
                                "index": "not_analyzed",
                            },
                            "SHARE_LIMIT": {
                                "type": "integer",
                                "null_value": 0
                            },
                            "SHARE_RATIO": {"type": "double"},
                            "SHARE_QUOTA": {
                                "type": "integer",
                                "null_value": 0
                            },
                            "TOTAL_DEMAND": {
                                "type": "integer",
                                "null_value": 0
                            },
                            "UNSATISFIED_DEMAND": {
                                "type": "integer",
                                "null_value": 0
                            },
                            "PLANNED_HOWN": {
                                "type": "integer",
                                "null_value": 0
                            },
                            "PLANNED_HRESERVE": {
                                "type": "integer",
                                "null_value": 0
                            },
                            "PLANNED_QUOTA": {
                                "type": "integer",
                                "null_value": 0
                            },
                        }
                    }
                }
        )

class ConsumerDemandLoader(Loader):
    """ Loader for the CONSUMER_DEMAN Table

    """
    def __init__(self, *args, **kwargs):
        super(ConsumerDemandLoader, self).__init__(*args,
                **kwargs)

    def _create_mapping(self):
        """ Create custom mapping for Consumer index
        """
        self.es.indices.put_mapping(
                index=self.index,
                doc_type=self.doctype,
                body={
                    self.doctype: {
                        "properties": {
                            "CLUSTER_NAME": {
                                "type": "string",
                                "index": "not_analyzed",
                            },
                            "TIME_STAMP": {
                                "type": "date",
                                "format": "dateOptionalTime"
                            },
                            "CONSUMER_NAME": {
                                "type": "string",
                                "index": "not_analyzed",
                            },
                            "MAX_REQUESTED": {
                                "type": "integer",
                                "null_value": 0
                            },
                            "USED": {
                                "type": "integer",
                                "null_value": 0
                            },
                            "INSERT_SEQ": {"type": "long"},
                        }
                    }
                }
        )
