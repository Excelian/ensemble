config = {
    'template_name': 'session_history',
    'all_index': 'session_history',
    'index_rollover': 'daily', # Other option is monthly
    'template_body': {
        'template': 'session_history*',
        'settings': {
            "number_of_shards": 1,
            "number_of_replicas": 1,
        },
        'aliases': {
            "session_history" : {},
        },
        'mappings': { 
            'default': {
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
    }
}
