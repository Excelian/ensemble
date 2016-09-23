config = {
    'template_name': 'consumer_demand',
    'all_index': 'consumer_demand',
    'index_rollover': 'daily', # Other option is monthly
    'template_body': {
        'template': 'consumer_demand*',
        'settings': {
            "number_of_shards": 1,
            "number_of_replicas": 1,
        },
        'aliases': {
            "consumer_demand" : {},
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
    }
}
