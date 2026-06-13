from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query, Body, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
from pydantic import BaseModel, Field
from typing import Any, Dict, List, Optional
import uvicorn
import json
import os
import re
from datetime import datetime

# Description shown to SAP Joule when it reads the OpenAPI spec.
# Joule uses this text to understand what the API does.
API_DESCRIPTION = """
Real-time event system for Daksha robots, callable from SAP Joule.

Use this API to assign tasks to a robot (inspection, pick & place), navigate a
robot to a location, emergency-stop or resume it, toggle its training pipeline,
upload a map's points-of-interest (POIs), send camera frames, or post any custom
event. The robot itself reports back when it reaches a destination. Every event
is stored and broadcast instantly to all connected WebSocket listeners. Each
event carries a metadata block describing where it came from and when it was
received.
"""

app = FastAPI(
    title="Daksha Robot Event API",
    version="2.8.0",
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
# Map / POI store
# robot_id -> [poi_name, ...]   (names only, as the robot sends to the nav API)
# Persisted to Supabase when configured; otherwise to disk under POI_DIR.
# POI_STORE is an in-memory cache used as a last-resort fallback.
# -----------------------------
POI_STORE: Dict[str, List[str]] = {}
POI_DIR = os.getenv("POI_DIR", "poi_data")
os.makedirs(POI_DIR, exist_ok=True)

# -----------------------------
# Supabase (optional, preferred map store)
# Set SUPABASE_URL and SUPABASE_KEY as environment variables on the server.
# NEVER hard-code the key. For a backend service, use the service_role key.
# If these are unset (or the supabase package isn't installed), the API
# transparently falls back to local-file storage under POI_DIR.
# -----------------------------
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
SUPABASE_TABLE = os.getenv("SUPABASE_TABLE", "robot_maps")

supabase = None
if SUPABASE_URL and SUPABASE_KEY:
    try:
        from supabase import create_client
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        print(f"[supabase] connected; map store -> table '{SUPABASE_TABLE}'")
    except Exception as e:  # package missing or bad config -> fall back to files
        print(f"[supabase] disabled, falling back to local files: {e}")
        supabase = None
else:
    print("[supabase] SUPABASE_URL/SUPABASE_KEY not set -> using local file map store")

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
    inspection_location: Optional[str] = Field(
        None, description="Inspection location (for inspection tasks), e.g. 'zone-C1'",
        examples=["zone-C1"],
    )
    status: Optional[str] = Field(
        None, description="Task/robot status, e.g. 'pending', 'in_progress', 'completed', 'idle'",
        examples=["pending"],
    )

    class Config:
        extra = "allow"  # arbitrary extra fields are still accepted and stored as data


class NavigateRequest(BaseModel):
    """Body for POST /event/navigate. Tells a robot to drive to a named location."""
    robot_id: str = Field(..., description="Target robot id, e.g. 'robot-01'", examples=["robot-01"])
    location: str = Field(..., description="Destination location name, e.g. 'zone-C1' or 'shelf-A3'", examples=["zone-C1"])
    metadata: Optional[Dict[str, Any]] = Field(None, description="Optional metadata object")

    class Config:
        extra = "allow"


class RobotStatusRequest(BaseModel):
    """Body for POST /event/robot_status. Emergency-stop or resume a robot."""
    robot_id: str = Field(..., description="Target robot id, e.g. 'robot-01'", examples=["robot-01"])
    emergency_stop: Optional[bool] = Field(None, description="Set true to immediately stop the robot", examples=[True])
    resume: Optional[bool] = Field(None, description="Set true to resume the robot after a stop", examples=[True])
    metadata: Optional[Dict[str, Any]] = Field(None, description="Optional metadata object")

    class Config:
        extra = "allow"


class TrainingModeRequest(BaseModel):
    """Body for POST /event/training-mode. Toggles the robot's training pipeline."""
    robot_id: str = Field(..., description="Target robot id, e.g. 'robot-01'", examples=["robot-01"])
    training: bool = Field(..., description="Training mode toggle: true = on, false = off", examples=[True])
    status: Optional[bool] = Field(None, description="Reported training status (true = active)", examples=[True])
    metadata: Optional[Dict[str, Any]] = Field(None, description="Optional metadata object")

    class Config:
        extra = "allow"


class LocationReachedRequest(BaseModel):
    """Body for POST /event/location_reached. The robot reports that it has
    arrived at its destination."""
    robot_id: str = Field(..., description="Reporting robot id, e.g. 'robot-01'", examples=["robot-01"])
    location: str = Field(..., description="The location the robot has reached, e.g. 'zone-C1'", examples=["zone-C1"])
    metadata: Optional[Dict[str, Any]] = Field(None, description="Optional metadata object")

    class Config:
        extra = "allow"


class MapEventRequest(BaseModel):
    """Body for POST /event/map. Uploads a map's point-of-interest (POI) NAMES for
    a robot — only the names, the same as the robot sends to the navigation API
    (x/y/yaw stay on the robot). Put the robot id in 'robot_id' and the POI names
    in 'data'. 'data' may be sent in any of these shapes:

      1) a plain list of names:
         ["kitchen", "reception"]

      2) the navigation payloads the robot already sends:
         [{"name": "kitchen", "robot": "RB9", "navigation_id": "R09-01"}, ...]

      3) the raw POI list (the server pulls out metadata.display_name):
         [{"metadata": {"display_name": "Kitchen"}, ...}, ...]

      4) a dict keyed by name (the server keeps the keys):
         {"kitchen": {...}, "reception": {...}}

    In every case the server keeps only the names and saves them to a JSON file."""
    robot_id: str = Field(..., description="Target robot id, e.g. 'robot-01'", examples=["robot-01"])
    data: Any = Field(
        ...,
        description="The POI names. A list of name strings, or objects/dicts the server extracts names from.",
    )
    metadata: Optional[Dict[str, Any]] = Field(None, description="Optional metadata object")

    class Config:
        extra = "allow"


class MapEventResponse(BaseModel):
    status: str = Field(..., description="success or error")
    event: str = Field(..., description="The event name ('map')")
    robot_id: str = Field(..., description="The robot the POIs belong to")
    message: str = Field(..., description="Human-readable result message")
    poi_count: int = Field(..., description="How many POI names were stored")
    # ["kitchen", "reception"] — POI names only
    names: List[str] = Field(..., description="The POI names that were stored")
    # [{name, robot, navigation_id}] — same shape the robot posts to the navigation API
    navigation: List[Dict[str, Any]] = Field(..., description="POIs as navigation payloads {name, robot, navigation_id}")
    saved_to: str = Field(..., description="Path of the JSON file the names were written to")
    metadata: Dict[str, Any] = Field(..., description="Event metadata: source, api_version, received_at, plus anything you sent")
    timestamp: str = Field(..., description="UTC time the POIs were stored")


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


def _safe_name(robot_id: str) -> str:
    """Make a robot id safe to use inside a filename."""
    return re.sub(r"[^A-Za-z0-9._-]", "_", robot_id) or "robot"


def normalize_poi_names(raw: Any) -> List[str]:
    """Extract just the POI names from whatever 'data' shape was sent, de-duplicated
    and order-preserving. Names are stripped and lower-cased, matching the robot's
    own POI handling. Accepts:
      - a list of name strings
      - a list of objects with 'name' or metadata.display_name
      - a dict keyed by name (keys are used)"""
    names: List[str] = []

    def _add(value: Any):
        clean = str(value).strip().lower()
        if clean and clean not in names:
            names.append(clean)

    if isinstance(raw, list):
        for item in raw:
            if isinstance(item, str):
                _add(item)
            elif isinstance(item, dict):
                name = item.get("name")
                if not name:
                    name = (item.get("metadata") or {}).get("display_name")
                if name:
                    _add(name)
    elif isinstance(raw, dict):
        for key in raw.keys():
            _add(key)

    return names


def poi_file_path(robot_id: str) -> str:
    """Path of the JSON file for a robot's POIs."""
    return os.path.join(POI_DIR, f"poi_{_safe_name(robot_id)}.json")


# ---- local-file backend (fallback when Supabase is not configured) ----
def _file_save(robot_id: str, names: List[str]) -> str:
    """Write POI names to POI_DIR/poi_<robot_id>.json as navigation payloads.
    Creates POI_DIR and the file if missing (e.g. fresh Render disk); overwrites
    if present. Returns the file path."""
    os.makedirs(POI_DIR, exist_ok=True)
    path = poi_file_path(robot_id)
    with open(path, "w") as f:
        json.dump(build_navigation(robot_id, names), f, indent=4)
    return path


def _file_load(robot_id: str) -> Optional[List[str]]:
    """Read POI names from disk, or None if no file exists."""
    path = poi_file_path(robot_id)
    if os.path.exists(path):
        with open(path, "r") as f:
            return normalize_poi_names(json.load(f))
    return None


def _file_delete(robot_id: str) -> bool:
    """Delete a robot's POI file. Returns True if a file was removed."""
    path = poi_file_path(robot_id)
    if os.path.exists(path):
        try:
            os.remove(path)
            return True
        except OSError:
            return False
    return False


# ---- public map store: Supabase if configured, else local file ----
def map_save(robot_id: str, names: List[str]) -> str:
    """Persist a robot's POI names and update the in-memory cache.
    Uses Supabase when configured, otherwise the local file. Returns a label
    describing where it was saved."""
    navigation = build_navigation(robot_id, names)
    POI_STORE[robot_id] = names  # always keep the cache warm

    if supabase is not None:
        try:
            supabase.table(SUPABASE_TABLE).upsert(
                {
                    "robot_id": robot_id,
                    "map_name": f"{robot_id}_map",
                    "pois": navigation,
                    "updated_at": datetime.utcnow().isoformat(),
                },
                on_conflict="robot_id",
            ).execute()
            return f"supabase:{SUPABASE_TABLE}"
        except Exception as e:
            # don't lose the map if Supabase hiccups -> fall back to a local file
            print(f"[supabase] save failed, falling back to file: {e}")

    return _file_save(robot_id, names)


def map_load(robot_id: str) -> List[str]:
    """Load a robot's POI names. Prefers Supabase, then the local file, then the
    in-memory cache. Returns [] if nothing is found."""
    if supabase is not None:
        try:
            result = (
                supabase.table(SUPABASE_TABLE)
                .select("*")
                .eq("robot_id", robot_id)
                .execute()
            )
            if result.data:
                names = normalize_poi_names(result.data[0].get("pois", []))
                POI_STORE[robot_id] = names
                return names
            return []
        except Exception as e:
            print(f"[supabase] load failed, falling back to file/cache: {e}")

    names = _file_load(robot_id)
    if names is None:
        names = POI_STORE.get(robot_id, [])
    else:
        POI_STORE[robot_id] = names
    return names


def map_delete(robot_id: str) -> Dict[str, Any]:
    """Delete a robot's map from Supabase (or the local file) and the in-memory
    cache. Returns what was removed."""
    had_cache = robot_id in POI_STORE
    name_count = len(POI_STORE.get(robot_id, []))
    POI_STORE.pop(robot_id, None)

    removed = False
    backend = "file"
    if supabase is not None:
        try:
            result = (
                supabase.table(SUPABASE_TABLE)
                .delete()
                .eq("robot_id", robot_id)
                .execute()
            )
            removed = bool(result.data)
            backend = "supabase"
        except Exception as e:
            print(f"[supabase] delete failed, falling back to file: {e}")
            removed = _file_delete(robot_id)
    else:
        removed = _file_delete(robot_id)

    return {
        "existed": removed or had_cache,
        "poi_count": name_count,
        "deleted": removed,
        "backend": backend,
    }


def build_navigation(robot_id: str, names: List[str]) -> List[Dict[str, Any]]:
    """Build the navigation-API payload list the robot sends:
    {name, robot, navigation_id}, with a unique id per POI."""
    return [
        {"name": name, "robot": robot_id, "navigation_id": f"{_safe_name(robot_id).upper()}-{i + 1:02d}"}
        for i, name in enumerate(names)
    ]


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
            "POST /event/navigate             (robot_id + location)",
            "POST /event/robot_status         (robot_id + emergency_stop / resume)",
            "POST /event/training-mode        (robot_id + training toggle + status)",
            "POST /event/location_reached     (robot_id + location  -> sent by the robot)",
            "POST /event/map                  (robot_id + data = POI names  -> saved to JSON)",
            "GET /event/map/{robot_id}        (saved map POI names for a robot)",
            "DELETE /event/map/{robot_id}     (delete a robot's saved map)",
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
                    "inspection_location": "zone-C1",
                    "status": "pending",
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
                    "status": "pending",
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
# Navigate a robot to a location
# -----------------------------
@app.post(
    "/event/navigate",
    response_model=EventResponse,
    summary="Navigate a robot to a location",
    description=(
        "Send a robot to a named destination. After it arrives, the robot reports "
        "back via POST /event/location_reached."
    ),
    operation_id="postNavigate",
    tags=["Events"],
)
async def post_navigate(
    payload: NavigateRequest = Body(
        ...,
        openapi_examples={
            "go_to_zone": {
                "summary": "Navigate to a zone",
                "value": {"robot_id": "robot-01", "location": "zone-C1", "metadata": {"source": "joule"}},
            },
        },
    )
):
    body = payload.model_dump(exclude_none=True)
    robot_id = str(body.pop("robot_id"))
    metadata = body.pop("metadata", None)
    # remaining fields (location + any extras) become the event data
    return await process_event(robot_id, "navigate", body, metadata, source="joule")


# -----------------------------
# Emergency stop / resume
# -----------------------------
@app.post(
    "/event/robot_status",
    response_model=EventResponse,
    summary="Emergency-stop or resume a robot",
    description=(
        "Control a robot's run state. Set emergency_stop=true to halt immediately, "
        "or resume=true to continue after a stop."
    ),
    operation_id="postRobotStatus",
    tags=["Events"],
)
async def post_robot_status(
    payload: RobotStatusRequest = Body(
        ...,
        openapi_examples={
            "stop": {"summary": "Emergency stop", "value": {"robot_id": "robot-01", "emergency_stop": True}},
            "resume": {"summary": "Resume", "value": {"robot_id": "robot-01", "resume": True}},
        },
    )
):
    body = payload.model_dump(exclude_none=True)
    robot_id = str(body.pop("robot_id"))
    metadata = body.pop("metadata", None)
    # remaining fields (emergency_stop / resume + any extras) become the event data
    return await process_event(robot_id, "robot_status", body, metadata, source="joule")


# -----------------------------
# Training pipeline toggle
# -----------------------------
@app.post(
    "/event/training-mode",
    response_model=EventResponse,
    summary="Toggle training mode",
    description=(
        "Turn the robot's training pipeline on or off. training=true starts training "
        "mode; training=false stops it. status reflects whether training is active."
    ),
    operation_id="postTrainingMode",
    tags=["Events"],
)
async def post_training_mode(
    payload: TrainingModeRequest = Body(
        ...,
        openapi_examples={
            "on": {"summary": "Training on", "value": {"robot_id": "robot-01", "training": True, "status": True}},
            "off": {"summary": "Training off", "value": {"robot_id": "robot-01", "training": False, "status": False}},
        },
    )
):
    body = payload.model_dump(exclude_none=True)
    robot_id = str(body.pop("robot_id"))
    metadata = body.pop("metadata", None)
    # remaining fields (training + status + any extras) become the event data
    return await process_event(robot_id, "training-mode", body, metadata, source="joule")


# -----------------------------
# Robot reports it reached the location
# -----------------------------
@app.post(
    "/event/location_reached",
    response_model=EventResponse,
    summary="Robot reports it reached a location",
    description=(
        "Posted by the robot once it has arrived at its destination. The event is "
        "stored and broadcast so Joule and any WebSocket listeners are notified."
    ),
    operation_id="postLocationReached",
    tags=["Events"],
)
async def post_location_reached(
    payload: LocationReachedRequest = Body(
        ...,
        openapi_examples={
            "arrived": {"summary": "Arrived at destination", "value": {"robot_id": "robot-01", "location": "zone-C1"}},
        },
    )
):
    body = payload.model_dump(exclude_none=True)
    robot_id = str(body.pop("robot_id"))
    metadata = body.pop("metadata", None)
    # source="robot" because this report comes from the robot, not Joule
    return await process_event(robot_id, "location_reached", body, metadata, source="robot")


# -----------------------------
# Map / POI upload
# (declared BEFORE the generic /event/{robot_id}/{event_name} routes so that
#  /event/map and /event/map/{robot_id} match here and are not swallowed by the
#  parameterised event routes.)
# -----------------------------
@app.post(
    "/event/map",
    response_model=MapEventResponse,
    summary="Upload a robot's map POI names",
    description=(
        "Upload the point-of-interest (POI) NAMES for a robot's map. Put the robot "
        "id in 'robot_id' and the names in 'data'. Only the names are kept (no "
        "x/y/yaw), matching what the robot sends to the navigation API. The names "
        "are saved to a JSON file as {name, robot, navigation_id} payloads, stored "
        "in memory, and broadcast to WebSocket listeners."
    ),
    operation_id="postRobotMap",
    tags=["Events"],
)
async def post_map(
    payload: MapEventRequest = Body(
        ...,
        openapi_examples={
            "name_list": {
                "summary": "Plain list of POI names",
                "value": {
                    "robot_id": "robot-01",
                    "data": ["kitchen", "reception", "warehouse"],
                    "metadata": {"source": "robot"},
                },
            },
            "navigation_payloads": {
                "summary": "Navigation payloads (as the robot sends them)",
                "value": {
                    "robot_id": "robot-01",
                    "data": [
                        {"name": "kitchen", "robot": "RB9", "navigation_id": "R09-01"},
                        {"name": "reception", "robot": "RB9", "navigation_id": "R09-02"},
                    ],
                    "metadata": {"source": "robot"},
                },
            },
            "raw_poi_list": {
                "summary": "Raw POI list (names pulled from metadata.display_name)",
                "value": {
                    "robot_id": "robot-01",
                    "data": [
                        {"metadata": {"display_name": "Kitchen"}},
                        {"metadata": {"display_name": "Reception"}},
                    ],
                    "metadata": {"source": "robot"},
                },
            },
        },
    )
):
    robot_id = str(payload.robot_id)
    names = normalize_poi_names(payload.data)
    if not names:
        raise HTTPException(
            status_code=422,
            detail=("No POI names found in 'data'. Send a list of name strings, "
                    "a list of objects with 'name' or metadata.display_name, "
                    "or a dict keyed by name."),
        )

    timestamp = datetime.utcnow().isoformat()
    metadata = build_metadata(payload.metadata, source="robot", timestamp=timestamp)

    saved_to = map_save(robot_id, names)
    navigation = build_navigation(robot_id, names)

    # store + broadcast as a normal event too, so /ws listeners and the event log see it
    event_data = {"names": names, "navigation": navigation, "poi_count": len(names)}
    store_event(robot_id, "map", event_data, metadata, timestamp)
    await manager.broadcast({
        "type": "event",
        "robot": robot_id,
        "event": "map",
        "data": event_data,
        "metadata": metadata,
        "timestamp": timestamp,
    })

    return MapEventResponse(
        status="success",
        event="map",
        robot_id=robot_id,
        message=f"Robot {robot_id}: {len(names)} POI names received and saved",
        poi_count=len(names),
        names=names,
        navigation=navigation,
        saved_to=saved_to,
        metadata=metadata,
        timestamp=timestamp,
    )


@app.get(
    "/event/map/{robot_id}",
    summary="Get a robot's saved map POI names",
    description="Returns the POI names saved for a robot, plus the navigation-payload form.",
    operation_id="getRobotMap",
    tags=["Events"],
)
async def get_map(robot_id: str):
    names = map_load(robot_id)
    return {
        "robot": robot_id,
        "poi_count": len(names),
        "names": names,
        "navigation": build_navigation(robot_id, names),
    }


@app.delete(
    "/event/map/{robot_id}",
    summary="Delete a robot's map POIs",
    description=(
        "Deletes the saved map POIs for a robot from the map store (Supabase if "
        "configured, otherwise the local JSON file) and from the in-memory cache. "
        "Broadcasts a 'map_deleted' event. Returns 200 whether or not a map existed "
        "(idempotent); check 'existed' in the response to see if anything was "
        "actually removed."
    ),
    operation_id="deleteRobotMap",
    tags=["Events"],
)
async def delete_map(robot_id: str):
    result = map_delete(robot_id)

    timestamp = datetime.utcnow().isoformat()
    metadata = build_metadata(None, source="api", timestamp=timestamp)
    event_data = {
        "deleted": result["deleted"],
        "poi_count": result["poi_count"],
        "backend": result["backend"],
    }
    # log + broadcast so listeners know the map was cleared
    store_event(robot_id, "map_deleted", event_data, metadata, timestamp)
    await manager.broadcast({
        "type": "event",
        "robot": robot_id,
        "event": "map_deleted",
        "data": event_data,
        "metadata": metadata,
        "timestamp": timestamp,
    })

    if result["existed"]:
        message = f"Robot {robot_id}: map deleted ({result['poi_count']} POIs removed)"
    else:
        message = f"Robot {robot_id}: no saved map found, nothing to delete"

    return {
        "status": "success",
        "event": "map_deleted",
        "robot_id": robot_id,
        "message": message,
        "existed": result["existed"],
        "poi_count": result["poi_count"],
        "deleted": result["deleted"],
        "backend": result["backend"],
        "metadata": metadata,
        "timestamp": timestamp,
    }


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
