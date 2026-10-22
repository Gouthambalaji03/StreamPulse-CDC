import asyncio
import datetime
import random
import time
import uuid
from typing import Dict, Any, List, Optional
from collections import deque
from backend.models import CDCEvent, ConnectorInfo, ConsumerGroup, TableSchema, MetricsSummary

class CDCEngine:
    def __init__(self):
        self.total_events = 12482
        self.insert_events = 6321
        self.update_events = 4186
        self.delete_events = 1975
        self.events_history = deque(maxlen=500)
        
        # High resolution time-series counters
        self.history_inserts = deque([random.randint(15, 60) for _ in range(16)], maxlen=24)
        self.history_updates = deque([random.randint(10, 45) for _ in range(16)], maxlen=24)
        self.history_deletes = deque([random.randint(4, 25) for _ in range(16)], maxlen=24)
        
        self.is_streaming = True
        self.stream_delay = 1.2  # seconds per event
        self.wal_base_offset = 0x1A4D800
        self.tx_id_seq = 10428
        self.kafka_offset_seq = 482910
        
        self.subscribers: List[asyncio.Queue] = []
        self._background_task: Optional[asyncio.Task] = None
        
        # Initial connectors
        self.connectors: Dict[str, ConnectorInfo] = {
            "postgres-source": ConnectorInfo(
                id="postgres-source",
                name="PostgreSQL WAL Source (Debezium)",
                type="source",
                technology="PostgreSQL 16 (Logical Replication)",
                status="RUNNING",
                events_processed=self.total_events,
                lag_ms=1.2,
                throughput_eps=18.4,
                target="kafka.cdc.postgres.public",
                icon="postgres"
            ),
            "kafka-broker": ConnectorInfo(
                id="kafka-broker",
                name="Kafka Event Streaming Broker",
                type="broker",
                technology="Apache Kafka 3.7",
                status="RUNNING",
                events_processed=self.total_events,
                lag_ms=0.4,
                throughput_eps=18.4,
                target="4 Topics / 12 Partitions",
                icon="kafka"
            ),
            "elasticsearch-sink": ConnectorInfo(
                id="elasticsearch-sink",
                name="Elasticsearch Search Index Sink",
                type="sink",
                technology="Elasticsearch 8.13",
                status="RUNNING",
                events_processed=self.total_events - 14,
                lag_ms=3.1,
                throughput_eps=18.1,
                target="indices: cdc_users, cdc_orders",
                icon="elasticsearch"
            ),
            "mongodb-sink": ConnectorInfo(
                id="mongodb-sink",
                name="MongoDB Analytics Data Lake Sink",
                type="sink",
                technology="MongoDB 7.0",
                status="RUNNING",
                events_processed=self.total_events - 8,
                lag_ms=2.6,
                throughput_eps=18.3,
                target="db: analytics_cdc",
                icon="mongodb"
            ),
        }
        
        # Consumer Groups
        self.consumer_groups: List[ConsumerGroup] = [
            ConsumerGroup(
                group_id="es-cdc-indexer-group",
                topic="cdc.postgres.public.events",
                partitions=4,
                current_offset=self.kafka_offset_seq - 14,
                log_end_offset=self.kafka_offset_seq,
                lag=14,
                state="Stable",
                members=3
            ),
            ConsumerGroup(
                group_id="mongo-analytics-pipeline",
                topic="cdc.postgres.public.events",
                partitions=4,
                current_offset=self.kafka_offset_seq - 8,
                log_end_offset=self.kafka_offset_seq,
                lag=8,
                state="Stable",
                members=2
            ),
            ConsumerGroup(
                group_id="realtime-dashboard-pusher",
                topic="cdc.postgres.public.events",
                partitions=4,
                current_offset=self.kafka_offset_seq,
                log_end_offset=self.kafka_offset_seq,
                lag=0,
                state="Stable",
                members=4
            ),
            ConsumerGroup(
                group_id="audit-log-archiver-s3",
                topic="cdc.postgres.public.events",
                partitions=4,
                current_offset=self.kafka_offset_seq - 31,
                log_end_offset=self.kafka_offset_seq,
                lag=31,
                state="Stable",
                members=2
            )
        ]
        
        # Prepopulate with exact matching events from screenshot and realistic stream
        self._seed_initial_events()

    def _seed_initial_events(self):
        # Screenshot specific items:
        # 10:24:31 | INSERT | users | 1024 | { "name": "John", ... }
        # 10:24:30 | UPDATE | orders | 8932 | { "status": "shipped", ... }
        # 10:24:28 | DELETE | products | 554 | { "id": 554 }
        # 10:24:27 | INSERT | payments | 7221 | { "amount": 129.99, ... }
        # 10:24:26 | UPDATE | users | 1023 | { "last_login": "2024-01-08..." }
        
        sample_rows = [
            ("INSERT", "users", 1024, {"name": "John Doe", "email": "john.doe@enterprise.io", "role": "engineer", "status": "active"}, None, "10:24:31"),
            ("UPDATE", "orders", 8932, {"status": "shipped", "carrier": "FedEx", "tracking": "FX-8849201"}, {"status": "processing", "carrier": None}, "10:24:30"),
            ("DELETE", "products", 554, {"id": 554, "sku": "PRD-554-BLK", "deleted_reason": "discontinued"}, {"name": "Wireless Mechanical Keyboard", "price": 149.00}, "10:24:28"),
            ("INSERT", "payments", 7221, {"amount": 129.99, "currency": "USD", "method": "stripe", "status": "captured"}, None, "10:24:27"),
            ("UPDATE", "users", 1023, {"last_login": "2024-01-08T10:24:26Z", "ip": "192.168.1.108"}, {"last_login": "2024-01-07T18:12:00Z"}, "10:24:26"),
            ("INSERT", "orders", 8933, {"customer_id": 1024, "amount": 284.50, "status": "pending", "items": 3}, None, "10:24:22"),
            ("UPDATE", "payments", 7220, {"status": "completed", "gateway_ref": "ch_3N82..."}, {"status": "pending"}, "10:24:19"),
            ("INSERT", "products", 555, {"name": "Ergonomic Monitor Arm Pro", "price": 89.99, "stock": 142}, None, "10:24:15"),
            ("DELETE", "orders", 8929, {"id": 8929, "reason": "fraudulent_attempt"}, {"customer_id": 994, "amount": 4900.00}, "10:24:10"),
            ("INSERT", "users", 1025, {"name": "Sophia Zhang", "email": "sophia.z@datastream.tech", "role": "admin"}, None, "10:24:04"),
        ]
        
        for op, table, rec_id, data, before, t_str in reversed(sample_rows):
            ev = self._create_event_object(op, table, rec_id, data, before, override_time=t_str)
            self.events_history.appendleft(ev)

    def _create_event_object(self, op: str, table: str, record_id: Any, data: Dict[str, Any], before: Optional[Dict[str, Any]] = None, override_time: Optional[str] = None) -> CDCEvent:
        now = datetime.datetime.now()
        time_str = override_time if override_time else now.strftime("%H:%M:%S")
        iso_str = now.isoformat()
        
        self.wal_base_offset += random.randint(32, 256)
        self.tx_id_seq += 1
        self.kafka_offset_seq += 1
        
        wal_lsn = f"0/{self.wal_base_offset:X}"
        partition = hash(str(record_id)) % 4
        
        after_data = data if op != "DELETE" else None
        before_data = before if before else ({"id": record_id} if op in ["UPDATE", "DELETE"] else None)
        
        event = CDCEvent(
            id=str(uuid.uuid4())[:8],
            time=time_str,
            timestamp=iso_str,
            ts_ms=int(time.time() * 1000),
            operation=op,
            table=table,
            schema_name="public",
            record_id=record_id,
            data=data,
            before=before_data,
            after=after_data,
            wal_lsn=wal_lsn,
            tx_id=self.tx_id_seq,
            kafka_topic=f"cdc.postgres.public.{table}",
            kafka_partition=partition,
            kafka_offset=self.kafka_offset_seq,
            sinks_synced=["elasticsearch", "mongodb"],
            latency_ms=round(random.uniform(1.2, 4.8), 2)
        )
        return event

    def generate_random_event(self) -> CDCEvent:
        operations = ["INSERT", "INSERT", "INSERT", "UPDATE", "UPDATE", "DELETE"]
        op = random.choice(operations)
        tables = ["users", "orders", "products", "payments", "inventory"]
        table = random.choice(tables)
        
        names = ["Alice Chen", "Marcus Vance", "Elena Rostova", "Liam O'Connor", "Maya Patel", "David Kim", "Sara Becker", "Carlos Mendez"]
        statuses = ["processing", "shipped", "delivered", "verified", "settled", "in_stock", "low_stock"]
        
        rec_id = random.randint(1000, 9999)
        before = None
        
        if table == "users":
            name = random.choice(names)
            email = f"{name.lower().replace(' ', '.')}@example.com"
            if op == "INSERT":
                data = {"name": name, "email": email, "role": "member", "status": "active"}
            elif op == "UPDATE":
                field = random.choice(["last_login", "status", "role"])
                val = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") if field == "last_login" else random.choice(["premium", "verified", "active"])
                data = {field: val, "user_id": rec_id}
                before = {field: "previous_state", "user_id": rec_id}
            else:
                data = {"id": rec_id, "deleted_at": datetime.datetime.now().isoformat()}
                
        elif table == "orders":
            if op == "INSERT":
                data = {"customer_id": random.randint(1000, 2000), "amount": round(random.uniform(19.99, 580.0), 2), "status": "pending", "items": random.randint(1, 5)}
            elif op == "UPDATE":
                st = random.choice(statuses)
                data = {"status": st, "updated_at": datetime.datetime.now().strftime("%H:%M:%S")}
                before = {"status": "pending"}
            else:
                data = {"id": rec_id, "reason": "user_cancelled"}
                
        elif table == "payments":
            if op == "INSERT":
                data = {"amount": round(random.uniform(15.0, 450.0), 2), "currency": "USD", "method": random.choice(["stripe", "apple_pay", "wire"]), "status": "captured"}
            elif op == "UPDATE":
                data = {"status": random.choice(["completed", "refunded", "disputed"])}
                before = {"status": "authorized"}
            else:
                data = {"id": rec_id, "voided": True}
                
        elif table == "products":
            if op == "INSERT":
                data = {"name": f"Item-{random.randint(100, 999)}", "price": round(random.uniform(25.0, 999.0), 2), "stock": random.randint(10, 500)}
            elif op == "UPDATE":
                data = {"stock": random.randint(0, 80), "price": round(random.uniform(25.0, 800.0), 2)}
                before = {"stock": 100}
            else:
                data = {"id": rec_id, "discontinued": True}
                
        else: # inventory
            data = {"warehouse": f"WH-{random.choice(['EAST', 'WEST', 'EU-1'])}", "quantity": random.randint(5, 200)}
            before = {"quantity": random.randint(201, 400)}

        event = self._create_event_object(op, table, rec_id, data, before)
        self.record_event(event)
        return event

    def record_event(self, event: CDCEvent):
        self.total_events += 1
        if event.operation == "INSERT":
            self.insert_events += 1
            self.history_inserts[-1] += 1
        elif event.operation == "UPDATE":
            self.update_events += 1
            self.history_updates[-1] += 1
        elif event.operation == "DELETE":
            self.delete_events += 1
            self.history_deletes[-1] += 1
            
        self.events_history.appendleft(event)
        
        # Update connector stats
        for conn in self.connectors.values():
            if conn.status == "RUNNING":
                conn.events_processed += 1
                conn.throughput_eps = round(random.uniform(16.5, 22.8), 1)
                conn.lag_ms = round(random.uniform(0.8, 4.2), 1)
                
        # Update consumer groups
        for cg in self.consumer_groups:
            cg.log_end_offset = self.kafka_offset_seq
            if random.random() < 0.8:
                cg.current_offset = max(0, self.kafka_offset_seq - random.randint(0, 15))
                cg.lag = cg.log_end_offset - cg.current_offset

    def mutate_table(self, table: str, operation: str, record_id: Optional[int] = None, data: Optional[Dict[str, Any]] = None) -> CDCEvent:
        rec_id = record_id if record_id else random.randint(1000, 9999)
        if not data:
            if operation == "INSERT":
                data = {"name": f"Custom {table.capitalize()}", "created_by": "Interactive UI", "value": round(random.uniform(50, 500), 2)}
            elif operation == "UPDATE":
                data = {"status": "manually_updated", "updated_at": datetime.datetime.now().strftime("%H:%M:%S")}
            else:
                data = {"id": rec_id, "deleted_by": "admin_ui"}
        
        before = {"status": "previous_state"} if operation in ["UPDATE", "DELETE"] else None
        event = self._create_event_object(operation.upper(), table, rec_id, data, before)
        self.record_event(event)
        return event

    def get_metrics(self) -> MetricsSummary:
        return MetricsSummary(
            total_events=self.total_events,
            insert_events=self.insert_events,
            update_events=self.update_events,
            delete_events=self.delete_events,
            eps=round(1.0 / self.stream_delay, 1) if self.is_streaming else 0.0,
            avg_latency_ms=2.4,
            active_connectors=len([c for c in self.connectors.values() if c.status == "RUNNING"]),
            sparkline_inserts=list(self.history_inserts),
            sparkline_updates=list(self.history_updates),
            sparkline_deletes=list(self.history_deletes),
            last_window="Last 5 minutes"
        )

    def get_schemas(self) -> List[TableSchema]:
        return [
            TableSchema(
                table_name="users",
                columns=[
                    {"name": "id", "type": "BIGSERIAL", "is_pk": True, "nullable": False},
                    {"name": "name", "type": "VARCHAR(128)", "is_pk": False, "nullable": False},
                    {"name": "email", "type": "VARCHAR(255)", "is_pk": False, "nullable": False},
                    {"name": "role", "type": "VARCHAR(32)", "is_pk": False, "nullable": True},
                    {"name": "status", "type": "VARCHAR(32)", "is_pk": False, "nullable": False},
                    {"name": "last_login", "type": "TIMESTAMPTZ", "is_pk": False, "nullable": True},
                    {"name": "created_at", "type": "TIMESTAMPTZ", "is_pk": False, "nullable": False},
                ],
                primary_key="id",
                total_records=14890,
                cdc_enabled=True,
                wal_replica_identity="FULL"
            ),
            TableSchema(
                table_name="orders",
                columns=[
                    {"name": "id", "type": "BIGSERIAL", "is_pk": True, "nullable": False},
                    {"name": "customer_id", "type": "BIGINT", "is_pk": False, "nullable": False},
                    {"name": "amount", "type": "NUMERIC(10,2)", "is_pk": False, "nullable": False},
                    {"name": "status", "type": "VARCHAR(32)", "is_pk": False, "nullable": False},
                    {"name": "items_count", "type": "INTEGER", "is_pk": False, "nullable": False},
                    {"name": "carrier", "type": "VARCHAR(64)", "is_pk": False, "nullable": True},
                    {"name": "tracking_num", "type": "VARCHAR(128)", "is_pk": False, "nullable": True},
                    {"name": "created_at", "type": "TIMESTAMPTZ", "is_pk": False, "nullable": False},
                ],
                primary_key="id",
                total_records=48120,
                cdc_enabled=True,
                wal_replica_identity="FULL"
            ),
            TableSchema(
                table_name="products",
                columns=[
                    {"name": "id", "type": "BIGSERIAL", "is_pk": True, "nullable": False},
                    {"name": "name", "type": "VARCHAR(255)", "is_pk": False, "nullable": False},
                    {"name": "sku", "type": "VARCHAR(64)", "is_pk": False, "nullable": False},
                    {"name": "price", "type": "NUMERIC(10,2)", "is_pk": False, "nullable": False},
                    {"name": "stock", "type": "INTEGER", "is_pk": False, "nullable": False},
                    {"name": "category", "type": "VARCHAR(64)", "is_pk": False, "nullable": True},
                ],
                primary_key="id",
                total_records=3400,
                cdc_enabled=True,
                wal_replica_identity="FULL"
            ),
            TableSchema(
                table_name="payments",
                columns=[
                    {"name": "id", "type": "BIGSERIAL", "is_pk": True, "nullable": False},
                    {"name": "order_id", "type": "BIGINT", "is_pk": False, "nullable": False},
                    {"name": "amount", "type": "NUMERIC(10,2)", "is_pk": False, "nullable": False},
                    {"name": "currency", "type": "VARCHAR(3)", "is_pk": False, "nullable": False},
                    {"name": "method", "type": "VARCHAR(32)", "is_pk": False, "nullable": False},
                    {"name": "status", "type": "VARCHAR(32)", "is_pk": False, "nullable": False},
                ],
                primary_key="id",
                total_records=46900,
                cdc_enabled=True,
                wal_replica_identity="FULL"
            ),
            TableSchema(
                table_name="inventory",
                columns=[
                    {"name": "id", "type": "BIGSERIAL", "is_pk": True, "nullable": False},
                    {"name": "product_id", "type": "BIGINT", "is_pk": False, "nullable": False},
                    {"name": "warehouse", "type": "VARCHAR(64)", "is_pk": False, "nullable": False},
                    {"name": "quantity", "type": "INTEGER", "is_pk": False, "nullable": False},
                    {"name": "reserved", "type": "INTEGER", "is_pk": False, "nullable": False},
                ],
                primary_key="id",
                total_records=12200,
                cdc_enabled=True,
                wal_replica_identity="FULL"
            ),
        ]

    async def broadcast_event(self, event: CDCEvent):
        msg = {
            "type": "NEW_EVENT",
            "event": event.dict(),
            "metrics": self.get_metrics().dict()
        }
        for q in list(self.subscribers):
            try:
                await q.put(msg)
            except Exception:
                pass

    async def start_streaming_worker(self):
        tick = 0
        while True:
            try:
                if self.is_streaming:
                    event = self.generate_random_event()
                    await self.broadcast_event(event)
                    
                    tick += 1
                    if tick % 10 == 0:
                        # Advance sparkline buckets
                        self.history_inserts.append(random.randint(15, 55))
                        self.history_updates.append(random.randint(10, 40))
                        self.history_deletes.append(random.randint(3, 20))
                
                await asyncio.sleep(self.stream_delay)
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"Error in streaming worker: {e}")
                await asyncio.sleep(1)

# Global singleton engine instance
engine = CDCEngine()
