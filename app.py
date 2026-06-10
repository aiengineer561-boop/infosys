from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query, Body, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
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


class RobotEventRequest(BaseModel):
    """Body for POST /event. Named fields here are what SAP Joule renders as
    input fields. Any extra fields you send (e.g. start_now, pick_location)
    are still accepted and stored as event data."""
    robot_id: str = Field(..., description="Target robot id, e.g. 'robot-01'", examples=["robot-01"])
    event: str = Field(..., description="Event name to post, e.g. 'task'", examples=["task"])
    metadata: Optional[Dict[str, Any]] = Field(
        None, description="Optional metadata object, e.g. {\"source\": \"joule\", \"priority\": \"high\"}"
    )
    task_type: Optional[str] = Field(
        None, description="Task type for the robot, e.g. 'inspection' or 'pick_and_place'",
        examples=["inspection"],
    )
    start_now: Optional[bool] = Field(None, description="Start the task immediately")
    pick_location: Optional[str] = Field(None, description="Pick location (for pick_and_place), e.g. 'shelf-A3'")
    place_location: Optional[str] = Field(None, description="Place location (for pick_and_place), e.g. 'bin-B7'")

    class Config:
        extra = "allow"  # arbitrary extra fields are still accepted and stored as data


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
    payload: RobotEventRequest = Body(
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
    # model_dump keeps both the named fields and any extra fields the caller sent.
    body = payload.model_dump(exclude_none=True)
    robot_id = str(body.pop("robot_id"))
    event_name = str(body.pop("event"))
    metadata = body.pop("metadata", None)
    if metadata is not None and not isinstance(metadata, dict):
        raise HTTPException(status_code=422, detail="'metadata' must be a JSON object.")
    # everything left over is the event data
    data = body
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
# OpenAPI 3.0.0 compatibility (SAP Joule reads /openapi.json)
# FastAPI/Pydantic v2 emit 3.1.0 by default, which SAP tooling rejects.
# We generate the spec, then down-convert the 3.1-only constructs to 3.0.0.
# -----------------------------
def _downconvert_to_30(node):
    """Recursively rewrite 3.1-only JSON-Schema constructs into 3.0.0 form."""
    if isinstance(node, list):
        return [_downconvert_to_30(n) for n in node]
    if not isinstance(node, dict):
        return node

    node = {k: _downconvert_to_30(v) for k, v in node.items()}

    # 1) type arrays e.g. ["string","null"] -> type:"string" + nullable:true
    if isinstance(node.get("type"), list):
        types = [t for t in node["type"] if t != "null"]
        if "null" in node["type"]:
            node["nullable"] = True
        if len(types) == 1:
            node["type"] = types[0]
        elif not types:
            node.pop("type", None)
        else:
            node.pop("type")
            node["anyOf"] = node.get("anyOf", []) + [{"type": t} for t in types]

    # 2) anyOf/oneOf containing {"type":"null"} -> drop it, mark nullable
    for key in ("anyOf", "oneOf"):
        if isinstance(node.get(key), list):
            non_null = [s for s in node[key] if s != {"type": "null"}]
            if len(non_null) != len(node[key]):
                node["nullable"] = True
            if len(non_null) == 1:
                only = non_null[0]
                node.pop(key)
                for k, v in only.items():
                    node.setdefault(k, v)
            else:
                node[key] = non_null

    # 3) numeric exclusiveMinimum/Maximum (3.1) -> boolean form (3.0)
    for bound, excl in (("minimum", "exclusiveMinimum"), ("maximum", "exclusiveMaximum")):
        if excl in node and isinstance(node[excl], (int, float)) and not isinstance(node[excl], bool):
            node[bound] = node[excl]
            node[excl] = True

    # 4) const (3.1) -> single-value enum (3.0)
    if "const" in node:
        node["enum"] = [node.pop("const")]

    # 5) schema-level `examples` array (3.1) -> single `example` (3.0)
    if isinstance(node.get("examples"), list):
        ex = node.pop("examples")
        if ex:
            node["example"] = ex[0]

    # 6) strip 3.1-only keywords that 3.0 validators reject
    for dead in ("$schema", "contentMediaType", "contentEncoding",
                 "unevaluatedProperties", "patternProperties", "$comment"):
        node.pop(dead, None)

    return node


def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    schema = get_openapi(
        title=app.title,
        version=app.version,
        description=app.description,
        contact=app.contact,
        servers=app.servers,
        routes=app.routes,
    )
    schema = _downconvert_to_30(schema)
    schema["openapi"] = "3.0.0"
    app.openapi_schema = schema
    return schema


app.openapi = custom_openapi


# -----------------------------
# Run
# -----------------------------
if __name__ == "__main__":
    import os
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("app:app", host="0.0.0.0", port=port)
