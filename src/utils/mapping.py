result_mapping = {
    "settings": {
        "analysis": { "analyzer": { "ko_standard": { "type": "standard" } } }
    },
    "mappings": {
        "dynamic": "true",
        "dynamic_templates": [
            {
                "response_fields_as_text": {
                    "path_match": "response.*",
                    "mapping": {
                        "type": "text",
                        "analyzer": "ko_standard",
                        "fields": { "keyword": { "type": "keyword", "ignore_above": 32766 } }
                    }
                }
            }
        ],
        "properties": {
            "rptc_id":   { "type": "keyword" },
            "rgtr_id":   { "type": "keyword" },
            "stdnt_id":  { "type": "keyword" },
            "response":  { "type": "object", "enabled": True },
            "feedback":  {
                "type": "text",
                "analyzer": "ko_standard",
                "fields": { "keyword": { "type": "keyword", "ignore_above": 32766 } }
            },
            "total_input_tokens":  { "type": "integer" },
            "total_output_tokens": { "type": "integer" },
            "total_tokens":        { "type": "integer" },
            "total_cost_krw":      { "type": "float" },
            "total_time_seconds":  { "type": "float" },
            "created_at": { "type": "date" }
        }
    }
}

TB_MBR_INFO = """
    CREATE TABLE 
"""

