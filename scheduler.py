# rule_evaluation.py

import logging
from datetime import datetime, timedelta
from postgres_utils import Postgres_DB, LOCATION_THRESHOLDS
import random
import asyncio


# Set up basic logging configuration (optional, if not already done in database.py)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)

LOGGER = logging.getLogger(__name__)

postgres_db = Postgres_DB()

def evaluate_rules(postgres_db):
    """Evaluate rules and create alerts if necessary."""
    
    
    try:
        start_time = datetime.now()
        end_time = start_time - timedelta(minutes=5)
        LOGGER.info(f"Evaluating rules between {end_time} and {start_time}")

        events = postgres_db.get_unsafe_events(start_time, end_time)
        
        for location_type, threshold in LOCATION_THRESHOLDS.items():
            location_events = [e for e in events if e[1] == location_type]  # e[1] is location_type
            for event in location_events:
                vehicle_id = event[0]  # e[0] is vehicle_id
                unsafe_events_count = event[2]  # e[2] is unsafe_events_count
                
                if unsafe_events_count >= threshold:
                    LOGGER.info(f'Unsafe driving conditions for {vehicle_id} driving at {location_type} with incident count = {unsafe_events_count}')
                    last_alert = postgres_db.get_last_alert(vehicle_id, start_time, end_time)
                    if not last_alert:
                        postgres_db.create_alert(vehicle_id, location_type, start_time)
                        LOGGER.info(f'creating new alert for {vehicle_id}')
                else:
                    LOGGER.info(f'Safe driving conditions for {vehicle_id}')
    finally:
        pass
        # postgres_db.close_connection()

def schedule_rule_evaluation_task(postgres_db):
    """Schedules the rule evaluation task to run periodically."""
    from fastapi_utils.tasks import repeat_every

    @repeat_every(seconds=60)  # Run every minute
    def periodic_task() -> None:
        evaluate_rules(postgres_db)


async def post_event_continuously(postgres_db):
    """Continuously post new events to the database every 10 seconds."""
    while True:
        try:
            # Create dummy event data
            timestamp = datetime.now()
            is_driving_safe = random.choice([True, False])  # Randomly generate True or False
            vehicle_id = "vehicle_x"
            location_type = "highway"  # or randomly select from valid location types
            
            #New event added
            LOGGER.info(f'Posting new event for {vehicle_id} -> at {location_type} where conditions are safe? {is_driving_safe}')
            # Insert the event into the database
            postgres_db.insert_event(timestamp, is_driving_safe, vehicle_id, location_type)

            # Wait for 10 seconds before posting the next event
            LOGGER.info(f'[{datetime.now()}] Waiting for 10 seconds...')
            await asyncio.sleep(10)
            LOGGER.info(f'[{datetime.now()}] Ready to post next event.')

        except Exception as e:
            print(f"Error posting event: {e}")

if __name__ == "__main__":
    # Optionally, you can run the evaluation once immediately
    evaluate_rules()

    # Or schedule it as a periodic task
    # schedule_rule_evaluation_task()
