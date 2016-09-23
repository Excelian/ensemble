config = {
    'template_name': 'consumer_resource_allocation',
    'all_index': 'consumer_resource_allocation',
    'index_rollover': 'daily', # Other option is monthly
    'template_body': {
        'template': 'consumer_resource_allocation*',
        'settings': {
            "number_of_shards": 1,
            "number_of_replicas": 1,
        },
        'aliases': {
            "consumer_resource_allocation" : {},
        },
        'mappings': { 
            'default': {
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
    }
}
