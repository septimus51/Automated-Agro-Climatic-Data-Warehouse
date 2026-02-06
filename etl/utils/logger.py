# etl/utils/logger.py
import logging
import uuid
from datetime import datetime
from typing import Optional

class ETLLogger:
    def __init__(self, name: str):
        self.logger = logging.getLogger(name)
        self.batch_id: Optional[str] = None
        
    def start_batch(self, pipeline_name: str) -> str:
        self.batch_id = f"{pipeline_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
        self.logger.info(f"Started batch: {self.batch_id}")
        return self.batch_id
    
    def log_extract(self, source: str, records: int):
        self.logger.info(f"[{self.batch_id}] Extracted {records} records from {source}")
    
    def log_transform(self, stage: str, records_in: int, records_out: int):
        self.logger.info(f"[{self.batch_id}] Transform {stage}: {records_in} -> {records_out}")
    
    def log_load(self, table: str, records: int, operation: str = 'INSERT'):
        self.logger.info(f"[{self.batch_id}] {operation} {records} records into {table}")
    
    def log_error(self, error: Exception, context: str = ""):
        self.logger.error(f"[{self.batch_id}] Error in {context}: {str(error)}", exc_info=True)