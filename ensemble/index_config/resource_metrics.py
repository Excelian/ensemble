config = {
    'template_name': 'resource_metrics',
    'all_index': 'resource_metrics',
    'index_rollover': 'daily', # Other option is monthly
    'template_body': {
        'template': 'resource_metrics*',
        'settings': {
            "number_of_shards": 1,
            "number_of_replicas": 1,
        },
        'aliases': {
            "resource_metrics" : {},
        },
        'mappings': { 
            'default': {
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
    }
}
