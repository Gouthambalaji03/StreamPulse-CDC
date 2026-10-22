import asyncio
import json
import os
from contextlib import asynccontextmanager
from typing import Optional, List

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from backend.cdc_engine import engine
from backend.models import MutationRequest, BatchRequest

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start the continuous CDC streaming worker task
    worker_task = asyncio.create_task(engine.start_streaming_worker())
    yield
    # Cleanup on shutdown
    worker_task.cancel()
    try:
        await worker_task
    except asyncio.CancelledError:
        pass

app = FastAPI(
    title="StreamPulse CDC Platform",
    description="Real-Time Database Change Streaming & Analytics Platform (PostgreSQL WAL -> Kafka -> Elasticsearch / MongoDB)",
    version="1.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# API Routes
@app.get("/api/health")
async def health_check():
    return {
        "status": "healthy",
        "service": "StreamPulse CDC Engine",
        "version": "1.0.0",
        "wal_replication": "ACTIVE",
        "kafka_broker": "CONNECTED",
        "active_subscribers": len(engine.subscribers)
    }

@app.get("/api/metrics")
async def get_metrics():
    return engine.get_metrics()

@app.get("/api/events")
async def get_events(
    limit: int = Query(50, ge=1, le=500),
    operation: Optional[str] = Query(None, description="Filter by INSERT, UPDATE, DELETE"),
    table: Optional[str] = Query(None, description="Filter by table name"),
    search: Optional[str] = Query(None, description="Search in data payload or record_id")
):
    results = list(engine.events_history)
    
    if operation and operation.upper() != "ALL":
        results = [e for e in results if e.operation.upper() == operation.upper()]
        
    if table and table.lower() != "all":
        results = [e for e in results if e.table.lower() == table.lower()]
        
    if search:
        s = search.lower()
        results = [
            e for e in results
            if s in str(e.record_id).lower() or s in str(e.data).lower() or s in e.table.lower()
        ]
        
    return results[:limit]

@app.post("/api/mutate")
async def create_mutation(req: MutationRequest):
    event = engine.mutate_table(
        table=req.table,
        operation=req.operation,
        record_id=req.record_id,
        data=req.data
    )
    await engine.broadcast_event(event)
    return {"status": "success", "event": event}

@app.post("/api/batch")
async def generate_batch(req: BatchRequest):
    generated = []
    for _ in range(min(req.count, 100)):
        ev = engine.generate_random_event()
        generated.append(ev)
        await engine.broadcast_event(ev)
        await asyncio.sleep(0.02)
    return {"status": "success", "count": len(generated)}

@app.get("/api/connectors")
async def get_connectors():
    return list(engine.connectors.values())

@app.post("/api/connectors/{connector_id}/toggle")
async def toggle_connector(connector_id: str):
    if connector_id not in engine.connectors:
        raise HTTPException(status_code=404, detail="Connector not found")
    c = engine.connectors[connector_id]
    c.status = "PAUSED" if c.status == "RUNNING" else "RUNNING"
    return c

@app.get("/api/consumers")
async def get_consumer_groups():
    return engine.consumer_groups

@app.get("/api/schemas")
async def get_schemas():
    return engine.get_schemas()

@app.post("/api/stream/control")
async def control_stream(action: str, speed_delay: Optional[float] = None):
    if action == "pause":
        engine.is_streaming = False
    elif action == "resume":
        engine.is_streaming = True
    elif action == "set_speed" and speed_delay is not None:
        engine.stream_delay = max(0.1, min(speed_delay, 10.0))
    return {
        "is_streaming": engine.is_streaming,
        "stream_delay": engine.stream_delay
    }

# WebSocket for real-time live events & metrics
@app.websocket("/ws/events")
async def websocket_events_endpoint(websocket: WebSocket):
    await websocket.accept()
    queue = asyncio.Queue(maxsize=100)
    engine.subscribers.append(queue)
    
    # Send initial state snapshot
    initial_payload = {
        "type": "INIT",
        "metrics": engine.get_metrics().dict(),
        "events": [e.dict() for e in list(engine.events_history)[:25]],
        "connectors": [c.dict() for c in engine.connectors.values()],
        "consumer_groups": [cg.dict() for cg in engine.consumer_groups],
        "is_streaming": engine.is_streaming,
        "stream_delay": engine.stream_delay
    }
    await websocket.send_json(initial_payload)
    
    async def listen_client():
        try:
            while True:
                data = await websocket.receive_json()
                action = data.get("action")
                if action == "SET_STREAMING":
                    engine.is_streaming = bool(data.get("value", True))
                elif action == "SET_SPEED":
                    engine.stream_delay = float(data.get("delay", 1.0))
                elif action == "MUTATE":
                    p = data.get("payload", {})
                    ev = engine.mutate_table(
                        table=p.get("table", "users"),
                        operation=p.get("operation", "INSERT"),
                        record_id=p.get("record_id"),
                        data=p.get("data")
                    )
                    await engine.broadcast_event(ev)
                elif action == "TRIGGER_BATCH":
                    count = int(data.get("count", 10))
                    for _ in range(min(count, 50)):
                        ev = engine.generate_random_event()
                        await engine.broadcast_event(ev)
                        await asyncio.sleep(0.04)
        except WebSocketDisconnect:
            pass
        except Exception:
            pass

    async def send_to_client():
        try:
            while True:
                msg = await queue.get()
                await websocket.send_json(msg)
        except WebSocketDisconnect:
            pass
        except Exception:
            pass

    client_task = asyncio.create_task(listen_client())
    sender_task = asyncio.create_task(send_to_client())
    
    done, pending = await asyncio.wait(
        [client_task, sender_task],
        return_when=asyncio.FIRST_COMPLETED
    )
    
    for task in pending:
        task.cancel()
        
    if queue in engine.subscribers:
        engine.subscribers.remove(queue)

# Mount frontend static files
frontend_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "frontend")
if os.path.exists(frontend_dir):
    app.mount("/static", StaticFiles(directory=frontend_dir), name="static")

@app.get("/")
async def serve_index():
    index_file = os.path.join(frontend_dir, "index.html")
    if os.path.exists(index_file):
        return FileResponse(index_file)
    return {"message": "StreamPulse CDC API is running. Frontend not found."}
