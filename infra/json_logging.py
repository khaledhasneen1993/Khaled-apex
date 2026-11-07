"""JSON stdout records without serializing arbitrary exception objects."""
import json
import logging
from datetime import datetime, timezone

class JsonFormatter(logging.Formatter):
    def format(self, record):
        return json.dumps({'timestamp': datetime.now(timezone.utc).isoformat(),
            'level': record.levelname, 'logger': record.name, 'message': record.getMessage()}, ensure_ascii=False)

def configure():
    for handler in logging.getLogger().handlers:
        handler.setFormatter(JsonFormatter())
