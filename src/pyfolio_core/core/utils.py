from pathlib import Path
from datetime import date, datetime
import json
import threading

from pyfolio_core.core.domainobjects import StockValue


class JSONLManager:
    
    def __init__(self, dir: Path, exchange: str, date_str: str, use_jsonl: bool = True):
        
        self.use_jsonl = use_jsonl
        self.file_path = dir / f"{date_str}_{exchange}.jsonl"
        self._lock = threading.Lock()

    def append(self, stock_value: StockValue):
        
        if not self.use_jsonl: return
        
        with self._lock:
            with open(self.file_path, 'a', encoding='utf-8') as f:
                f.write(json.dumps(stock_value.to_dict()) + '\n')
                
def json_serial(obj):
    
    """Converts objects that the JSON library doesn't recognize into strings."""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")     