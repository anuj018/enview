from fastapi import FastAPI, HTTPException
import postgres_utils  # Import your Postgres_DB class from postgres_utils
from scheduler import (evaluate_rules,# Import evaluate_rules from scheduler.py
                       post_event_continuously)  #Adds new events to database
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
import asyncio

app = FastAPI()

# Initialize the Postgres_DB instance
postgres_db = postgres_utils.Postgres_DB()

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Application startup")

    # Check if tables exist and create them if they don't
    postgres_db.check_and_create_tables("db.sql")

    # Create an instance of the scheduler
    scheduler = AsyncIOScheduler()

    # Add the periodic job to the scheduler (runs every 60 seconds)
    scheduler.add_job(evaluate_rules, IntervalTrigger(seconds=60), args=[postgres_db])

    # Start the scheduler
    scheduler.start()

    task = asyncio.create_task(post_event_continuously(postgres_db))

    yield  # The application runs during this period

    # Stop the background task
    task.cancel()
    await task

    # Shutdown the scheduler and close the database connection
    scheduler.shutdown(wait=False)
    postgres_db.close_connection()
    print("Application shutdown")

app = FastAPI(lifespan=lifespan)

@app.post("/events/")
def create_event(timestamp: datetime, is_driving_safe: bool, vehicle_id: str, location_type: str):
    try:
        postgres_db.insert_event(timestamp, is_driving_safe, vehicle_id, location_type)
        return {"message": "Event recorded successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error recording event: {e}")

@app.get("/alerts/")
def get_alerts(start_time: datetime, end_time: datetime, vehicle_id: str = None):
    try:
        alerts = postgres_db.get_alerts(start_time=start_time, end_time=end_time, vehicle_id=vehicle_id)
        return alerts
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving alerts: {e}")

@app.get("/recent-alerts/")
def get_recent_alerts(vehicle_id: str = None):
    try:
        start_time = datetime.now() - timedelta(minutes=5)
        end_time = datetime.now()

        alerts = postgres_db.get_alerts(start_time=start_time, end_time=end_time, vehicle_id=vehicle_id)
        return alerts
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving recent alerts: {e}")
    

@app.get("/last-alert/")
def get_last_alert(vehicle_id: str, start_time: datetime, end_time: datetime):
    try:
        last_alert = postgres_db.get_last_alert(vehicle_id=vehicle_id, start_time=start_time, end_time=end_time)
        if last_alert:
            return {"alert_id": last_alert[0]}
        else:
            return {"message": "No alerts found in the specified time window."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving the last alert: {e}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
