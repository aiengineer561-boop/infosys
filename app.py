from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query, Body, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Any, Dict, List, Optional
import uvicorn
import json
from datetime import datetime

# Description shown to SAP Joule when it reads the OpenAPI spec.
# Joule uses this text to understand what the API does.
API_DESCRIPTION = """
Real-time event system for Daksha robots, callable from SAP Joule.

Use this API to assign tasks to a robot (inspection, pick & place), send
camera frames, or post any custom event. Every event is stored and broadcast
instantly to all connected WebSocket listeners. Each event carries a metadata
block describing where it came from and when it was received.
"""

app = FastAPI(
    title="Daksha Robot Event API",
    version="2.4.0",
    description=API_DESCRIPTION,
    contact={"name": "Daksha", "url": "https://tara-gen-1v2.onrender.com"},
    servers=[
        {"url": "https://tara-gen-1v2.onrender.com", "description": "Production"}
    ],
)

# -----------------------------
# CORS
# -----------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------
# In-memory event store
# robot_id -> list of events
# -----------------------------
EVENT_STORE: Dict[str, List[Dict[str, Any]]] = {}

# -----------------------------
# WebSocket Manager
# -----------------------------
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        for ws in self.active_connections:
            try:
                await ws.send_json(message)
            except:
                pass


manager = ConnectionManager()

# -----------------------------
# Models  (clear schemas -> Joule reads these as input/output fields)
# -----------------------------
class EventPayload(BaseModel):
    """Body for the path-based endpoint. Anything you send is kept as data."""
    class Config:
        extra = "allow"


class EventResponse(BaseModel):
    status: str = Field(..., description="success or error")
    event: str = Field(..., description="The event name that was posted")
    # Joule surfaces this 'message' field back to the user.
    message: str = Field(..., description="Human-readable result message")
    data: Optional[Dict[str, Any]] = Field(None, description="The event payload data")
    metadata: Dict[str, Any] = Field(..., description="Info about the event: source, api_version, received_at, plus anything you sent")
    timestamp: str = Field(..., description="UTC time the event was stored")


# Keys that have special meaning inside a body payload.
ROBOT_KEYS = {"robot_id", "robot", "robotId"}
EVENT_KEYS = {"event", "eventname", "event_name", "eventName"}
META_KEYS = {"metadata", "meta"}
RESERVED_KEYS = ROBOT_KEYS | EVENT_KEYS | META_KEYS


# -----------------------------
# Helpers
# -----------------------------
def build_metadata(incoming: Optional[Dict[str, Any]], source: str, timestamp: str) -> Dict[str, Any]:
    """Server-generated metadata, merged with whatever the caller sent."""
    meta = {
        "api_version": app.version,
        "source": source,
        "received_at": timestamp,
    }
    if incoming:
        meta.update(incoming)
    return meta


def store_event(robot_id: str, event_name: str, data: Dict[str, Any],
                metadata: Dict[str, Any], timestamp: str):
    EVENT_STORE.setdefault(robot_id, []).append({
        "event": event_name,
        "data": data,
        "metadata": metadata,
        "timestamp": timestamp
    })


async def process_event(robot_id: str, event_name: str, data: Dict[str, Any],
                        incoming_meta: Optional[Dict[str, Any]], source: str) -> EventResponse:
    """Shared logic: build metadata, store the event, broadcast it, return a response."""
    timestamp = datetime.utcnow().isoformat()
    metadata = build_metadata(incoming_meta, source, timestamp)

    store_event(robot_id, event_name, data, metadata, timestamp)

    await manager.broadcast({
        "type": "event",
        "robot": robot_id,
        "event": event_name,
        "data": data,
        "metadata": metadata,
        "timestamp": timestamp
    })

    return EventResponse(
        status="success",
        event=event_name,
        message=f"Robot {robot_id}: event '{event_name}' received",
        data={"robot": robot_id, **data} if data else {"robot": robot_id},
        metadata=metadata,
        timestamp=timestamp
    )


def extract_from_body(payload: Dict[str, Any]):
    """Pull robot_id, event_name and metadata out of a raw body dict.
    Everything else becomes the event data."""
    robot_id = next((str(payload[k]) for k in ROBOT_KEYS if payload.get(k)), None)
    event_name = next((str(payload[k]) for k in EVENT_KEYS if payload.get(k)), None)
    metadata = next((payload[k] for k in META_KEYS if payload.get(k)), None)

    if not robot_id:
        raise HTTPException(status_code=422,
            detail="Missing robot id in body. Add one of: robot_id, robot.")
    if not event_name:
        raise HTTPException(status_code=422,
            detail="Missing event name in body. Add one of: event, eventname, event_name.")
    if metadata is not None and not isinstance(metadata, dict):
        raise HTTPException(status_code=422,
            detail="'metadata' must be a JSON object.")

    data = {k: v for k, v in payload.items() if k not in RESERVED_KEYS}
    return robot_id, event_name, metadata, data


# -----------------------------
# Root
# -----------------------------
@app.get("/", summary="API info", operation_id="getApiInfo", tags=["Info"])
async def root():
    return {
        "name": "Daksha Robot Event API",
        "version": app.version,
        "endpoints": [
            "POST /event                      (robot_id + event + metadata inside the body)",
            "POST /event/{robot_id}/{event_name}",
            "GET /event/{robot_id}",
            "GET /event/{robot_id}/{event_name}",
            "WebSocket /ws"
        ]
    }


# -----------------------------
# Main endpoint for Joule: everything in the body
# -----------------------------
@app.post(
    "/event",
    response_model=EventResponse,
    summary="Post a robot event",
    description=(
        "Post an event to a robot. The robot id, the event name, and an optional "
        "metadata object all go inside the JSON body. Any other fields you include "
        "are stored as the event data. Use this to assign inspection or pick & place "
        "tasks, send camera frames, or trigger any custom event."
    ),
    operation_id="postRobotEvent",
    tags=["Events"],
)
async def post_event_body(
    payload: Dict[str, Any] = Body(
        ...,
        openapi_examples={
            "inspection_now": {
                "summary": "Start inspection now",
                "value": {
                    "robot_id": "robot-01",
                    "event": "task",
                    "metadata": {"source": "joule", "priority": "high"},
                    "task_type": "inspection",
                    "start_now": True,
                },
            },
            "pick_and_place": {
                "summary": "Pick and place",
                "value": {
                    "robot_id": "robot-01",
                    "event": "task",
                    "metadata": {"source": "joule"},
                    "task_type": "pick_and_place",
                    "pick_location": "shelf-A3",
                    "place_location": "bin-B7",
                },
            },
        },
    )
):
    robot_id, event_name, metadata, data = extract_from_body(payload)
    return await process_event(robot_id, event_name, data, metadata, source="joule")


# -----------------------------
# GET robot events
# -----------------------------
@app.get("/event/{robot_id}", summary="Get recent events for a robot",
         operation_id="getRobotEvents", tags=["Events"])
async def get_robot_events(
    robot_id: str,
    limit: int = Query(20, ge=1, le=100)
):
    events = EVENT_STORE.get(robot_id, [])[-limit:]
    return {"robot": robot_id, "events": events, "count": len(events)}


@app.get("/event/{robot_id}/{event_name}", summary="Get events of one type for a robot",
         operation_id="getRobotEventsByName", tags=["Events"])
async def get_robot_event_by_name(
    robot_id: str,
    event_name: str,
    limit: int = Query(20, ge=1, le=100)
):
    events = [e for e in EVENT_STORE.get(robot_id, []) if e["event"] == event_name][-limit:]
    return {"robot": robot_id, "event": event_name, "events": events, "count": len(events)}


# -----------------------------
# POST robot event (old path-based way, still works)
# -----------------------------
@app.post("/event/{robot_id}/{event_name}", response_model=EventResponse,
          summary="Post an event (path-based)", operation_id="postRobotEventByPath",
          tags=["Events"])
async def post_robot_event(
    robot_id: str,
    event_name: str,
    payload: EventPayload
):
    body = payload.model_dump()
    incoming_meta = body.pop("metadata", None) or body.pop("meta", None)
    if incoming_meta is not None and not isinstance(incoming_meta, dict):
        incoming_meta = None
    return await process_event(robot_id, event_name, body, incoming_meta, source="api")


# -----------------------------
# WebSocket
# -----------------------------
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)


# -----------------------------
# Health
# -----------------------------
@app.get("/health", summary="Health check", operation_id="getHealth", tags=["Info"])
async def health():
    return {
        "status": "ok",
        "robots": len(EVENT_STORE),
        "connections": len(manager.active_connections)
    }


# -----------------------------
# Run
# -----------------------------
if __name__ == "__main__":
    import os
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("app:app", host="0.0.0.0", port=port)
