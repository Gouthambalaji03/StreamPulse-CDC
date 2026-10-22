from typing import Dict, Any, Optional, List
from pydantic import BaseModel, Field
import time

class CDCEvent(BaseModel):
    id: str
    time: str = Field(description="Formatted HH:MM:SS timestamp")
    timestamp: str = Field(description="ISO timestamp")
    ts_ms: int = Field(default_factory=lambda: int(time.time() * 1000))
    operation: str = Field(description="INSERT | UPDATE | DELETE")
    table: str = Field(description="Source table name")
    schema_name: str = "public"
    record_id: Any = Field(description="Primary key / identifier")
    data: Dict[str, Any] = Field(description="Current payload snapshot")
    before: Optional[Dict[str, Any]] = None
    after: Optional[Dict[str, Any]] = None
    wal_lsn: str = "0/1A4D800"
    tx_id: int = 10428
    kafka_topic: str = "cdc.postgres.public.events"
    kafka_partition: int = 0
    kafka_offset: int = 0
    sinks_synced: List[str] = ["elasticsearch", "mongodb"]
    latency_ms: float = 2.4

class MutationRequest(BaseModel):
    table: str = "orders"
    operation: str = "INSERT" # INSERT, UPDATE, DELETE
    record_id: Optional[int] = None
    data: Optional[Dict[str, Any]] = None

class BatchRequest(BaseModel):
    count: int = 50
    table: Optional[str] = None

class ConnectorInfo(BaseModel):
    id: str
    name: str
    type: str  # source | broker | sink
    technology: str
    status: str  # RUNNING | PAUSED | DEGRADED
    events_processed: int
    lag_ms: float
    throughput_eps: float
    target: str
    icon: str

class ConsumerGroup(BaseModel):
    group_id: str
    topic: str
    partitions: int
    current_offset: int
    log_end_offset: int
    lag: int
    state: str
    members: int

class TableSchema(BaseModel):
    table_name: str
    columns: List[Dict[str, Any]]
    primary_key: str
    total_records: int
    cdc_enabled: bool = True
    wal_replica_identity: str = "FULL"

class MetricsSummary(BaseModel):
    total_events: int
    insert_events: int
    update_events: int
    delete_events: int
    eps: float
    avg_latency_ms: float
    active_connectors: int
    sparkline_inserts: List[int]
    sparkline_updates: List[int]
    sparkline_deletes: List[int]
    last_window: str = "Last 5 minutes"
