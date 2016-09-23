config = {
    'template_name': 'session_attributes',
    'all_index': 'session_attributes',
    'index_rollover': 'daily', # Other option is monthly
    'template_body': {
        'template': 'session_attributes*',
        'settings': {
            "number_of_shards": 1,
            "number_of_replicas": 1,
        },
        'aliases': {
            "session_attributes" : {},
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
    }
}
