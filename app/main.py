import time
import traceback
from typing import Dict, List, Optional
from collections import defaultdict

from fastapi import FastAPI, Request, status, Query
from fastapi.exceptions import RequestValidationError
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel


# --- Models ---

class BotStats(BaseModel):
    bot_id: str
    received: int
    processed: int
    in_flight: int
    throughput: float
    elapsed: float
    empty_polls: int
    partitions: int
    progress: float
    timestamp: float
    
    class Config:
        # allow extra fields in the incoming JSON
        extra = "allow"


# --- App Setup ---

app = FastAPI(title="Bot Stats Dashboard", version="1.0.0")

# Mount static and templates
app.mount("/static", StaticFiles(directory="app/static"), name="static")
templates = Jinja2Templates(directory="app/templates")

# In-memory storage
stats_store: Dict[str, dict] = {}
history_store: List[dict] = []

# Config
MAX_HISTORY_ENTRIES = 100
ACTIVE_BOT_TIMEOUT = 15  # seconds


# --- Helper Functions ---

def compute_global_stats(all_stats: dict) -> dict:
    """Calculate aggregate statistics across all active bots."""
    if not all_stats:
        return {
            "bots": 0,
            "received": 0,
            "processed": 0,
            "in_flight": 0,
            "empty_polls": 0, 
            "partitions": 0,
            "elapsed": 0,
            "throughput": 0,
            "progress": 0
        }
    
    total_received = sum(bot["received"] for bot in all_stats.values())
    total_processed = sum(bot["processed"] for bot in all_stats.values())
    total_in_flight = sum(bot["in_flight"] for bot in all_stats.values())
    total_empty_polls = sum(bot["empty_polls"] for bot in all_stats.values())
    total_partitions = sum(bot["partitions"] for bot in all_stats.values())
    total_elapsed = max(bot["elapsed"] for bot in all_stats.values())
    all_throughput = sum(
        bot["throughput"] for bot in all_stats.values() if bot["throughput"]
    )
    progress = (total_processed / total_received * 100) if total_received else 0

    return {
        "bots": len(all_stats),
        "received": total_received,
        "processed": total_processed,
        "in_flight": total_in_flight,
        "empty_polls": total_empty_polls,
        "partitions": total_partitions,
        "elapsed": total_elapsed,
        "throughput": all_throughput,
        "progress": progress,
    }


def get_active_bots():
    """Return only bots that have been updated recently."""
    now = time.time()
    return {
        bot_id: data
        for bot_id, data in stats_store.items()
        if now - data.get("timestamp", 0) <= ACTIVE_BOT_TIMEOUT
    }


def aggregate_bots(bot_list: List[dict], by_field: str) -> List[dict]:
    """
    Aggregate bot statistics by specified field (ip_address, topic, group_id)
    """
    if not by_field or by_field == "none":
        return bot_list
        
    aggregated = defaultdict(lambda: {
        "received": 0,
        "processed": 0,
        "erred": 0,
        "in_flight": 0,
        "empty_polls": 0,
        "partitions": 0,
        "throughput": 0,
        "queue_size": 0,
        "transactions": 0,
        "bots": []
    })
    
    for bot in bot_list:
        # Skip bots missing the aggregation field
        if by_field not in bot:
            continue
            
        key = bot[by_field]
        group = aggregated[key]
        
        # Aggregate numeric fields
        group["received"] += bot.get("received", 0)
        group["processed"] += bot.get("processed", 0)
        group["erred"] += bot.get("erred", 0) if "erred" in bot else 0
        group["in_flight"] += bot.get("in_flight", 0)
        group["empty_polls"] += bot.get("empty_polls", 0)
        group["partitions"] += bot.get("partitions", 0)
        group["throughput"] += bot.get("throughput", 0)
        group["queue_size"] += bot.get("queue_size", 0) if "queue_size" in bot else 0
        group["transactions"] += bot.get("transactions", 0) if "transactions" in bot else 0
        
        # Track bot IDs in this group
        group["bots"].append(bot["bot_id"])
        
        # Use the most recent timestamp
        if "timestamp" not in group or bot.get("timestamp", 0) > group["timestamp"]:
            group["timestamp"] = bot.get("timestamp", 0)
            
        # Copy non-aggregated fields from the first bot in group
        if "topic" not in group and "topic" in bot:
            group["topic"] = bot["topic"]
        if "group_id" not in group and "group_id" in bot:
            group["group_id"] = bot["group_id"]
        if "ip_address" not in group and "ip_address" in bot:
            group["ip_address"] = bot["ip_address"]
        if "register_at" not in group and "register_at" in bot:
            group["register_at"] = bot["register_at"]
            
    # Create result list with the aggregation field as bot_id
    result = []
    for key, data in aggregated.items():
        result.append({
            "bot_id": f"{key} ({len(data['bots'])} bots)",
            "bots_count": len(data['bots']),
            "bots": data["bots"],
            **data
        })
    
    return result


def update_history(bot_list, active_bot_ids):
    """Update history with new snapshot and filter outdated data."""
    now = time.time()
    
    # Record a new timestamped snapshot
    snapshot = {"timestamp": now, "stats": bot_list}
    
    # Filter existing history to keep only active bots
    for entry in history_store:
        entry["stats"] = [
            stat for stat in entry["stats"] 
            if stat["bot_id"] in active_bot_ids
        ]
    
    # Add new snapshot and maintain history size limit
    history_store.append(snapshot)
    if len(history_store) > MAX_HISTORY_ENTRIES:
        history_store.pop(0)


# --- Error Handlers ---

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(
    request: Request, exc: RequestValidationError
):
    print("❌ Validation error:")
    print("URL:", request.url)
    print("Body:", await request.body())
    print("Errors:", exc.errors())
    print("Trace:", traceback.format_exc())

    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={"detail": exc.errors()},
    )


# --- API Routes ---

@app.post("/update")
def update_stats(stat: BotStats, request: Request):
    """Update stats for a single bot, including its IP address."""
    stats_dict = stat.dict()
    stats_dict["ip_address"] = request.client.host
    stats_store[stat.bot_id] = stats_dict
    return {"status": "ok"}


@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request):
    """Render the dashboard HTML page."""
    return templates.TemplateResponse(
        "index.html", {"request": request, "build_id": int(time.time())}
    )


@app.get("/api/stats")
def api_stats(aggregate_by: Optional[str] = Query(None, description="Field to aggregate by: ip_address, topic, group_id, or none")):
    """
    Return both per-bot and global stats as JSON, plus history.
    Optional aggregation by IP address, topic, or group_id.
    """
    fresh_stats = get_active_bots()
    global_stats = compute_global_stats(fresh_stats)
    
    # Sort bots by throughput, highest first
    sorted_stats = sorted(
        fresh_stats.items(),
        key=lambda x: x[1]["throughput"],
        reverse=True,
    )
    
    # Transform into a list of dicts including bot_id
    bot_list = [{"bot_id": bot_id, **data} for bot_id, data in sorted_stats]
    
    # Aggregate bots if requested
    if aggregate_by in ["ip_address", "topic", "group_id"]:
        bot_list = aggregate_bots(bot_list, aggregate_by)
        # Re-sort after aggregation
        bot_list = sorted(bot_list, key=lambda x: x["throughput"], reverse=True)
    
    # Update history with active bots only (before aggregation)
    active_bot_ids = set(fresh_stats.keys())
    update_history([{"bot_id": bot_id, **data} for bot_id, data in sorted_stats], active_bot_ids)

    return {
        "global": global_stats, 
        "stats": bot_list, 
        "history": history_store,
        "aggregated_by": aggregate_by
    }
