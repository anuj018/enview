import logging
import psycopg2
from psycopg2 import sql
from config import config
from datetime import datetime, timedelta


# Set up basic logging configuration
logging.basicConfig(
    level=logging.INFO,  # Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",  # Log message format
    handlers=[
        logging.StreamHandler()  # Logs to the console
    ]
)

LOGGER = logging.getLogger(__name__)

POSTGRES_CONFIG = config.get("postgres", {})
LOCATION_THRESHOLDS = config.get("location_thresholds")
VALID_LOCATION_TYPES = config.get("valid_location_types")
class Postgres_DB:
    def __init__(self) -> None:
        """Creates a connection to the PostgreSQL database mentioned in the configs."""
        try:
            self.db_connection = psycopg2.connect(
                host=POSTGRES_CONFIG.get("url", "localhost"),
                port=POSTGRES_CONFIG.get("port", 5432),
                database=POSTGRES_CONFIG.get("database", "enview"),
                user=POSTGRES_CONFIG.get("username", "postgres"),
                password=POSTGRES_CONFIG.get("password", "postgres"),
            )
            database = POSTGRES_CONFIG.get("database")
            LOGGER.info(f"Successfully connected to DB {database}")
        except Exception as e:
            LOGGER.error("Failed to connect to the database", exc_info=True)
            self.db_connection = None
    
    def close_connection(self) -> None:
        """Closes the database connection."""
        try:
            if self.db_connection:
                self.db_connection.close()
                LOGGER.info("DB connection closed")
        except Exception as e:
            LOGGER.error("Unable to close the DB connection", exc_info=True)

    def execute_sql_file(self, sql_file_path):
        """Execute the provided SQL file to set up the database."""
        cursor = None
        try:
            with open(sql_file_path, 'r') as sql_file:
                sql_commands = sql_file.read()

            cursor = self.db_connection.cursor()
            cursor.execute(sql_commands)
            self.db_connection.commit()
            LOGGER.info(f"Executed SQL file: {sql_file_path}")

        except Exception as e:
            LOGGER.error(f"Failed to execute SQL file: {e}", exc_info=True)
            self.db_connection.rollback()
        finally:
            if cursor:
                cursor.close()

    def get_unsafe_events(self, start_time, end_time):
        """Retrieve unsafe events within a specific time window."""
        cursor = self.db_connection.cursor()
        try:
            query = sql.SQL("""
                SELECT vehicle_id, location_type, COUNT(DISTINCT timestamp) AS unsafe_events_count
                FROM events
                WHERE is_driving_safe = FALSE
                AND timestamp BETWEEN %s AND %s
                GROUP BY vehicle_id, location_type
            """)
            cursor.execute(query, (end_time, start_time))
            events = cursor.fetchall()
            return events
        except Exception as e:
            LOGGER.error("Failed to retrieve unsafe events", exc_info=True)
            return []
        finally:
            cursor.close()

    def get_last_alert(self, vehicle_id, start_time, end_time):
        """Check if an alert already exists for a vehicle in the current time window."""
        cursor = self.db_connection.cursor()
        try:
            query = sql.SQL("""
                SELECT id FROM alerts 
                WHERE vehicle_id = %s 
                AND timestamp BETWEEN %s AND %s
                LIMIT 1
            """)
            cursor.execute(query, (vehicle_id, end_time, start_time))  # Checking within the time window
            last_alert = cursor.fetchone()
            return last_alert
        except Exception as e:
            LOGGER.error("Failed to check for last alert", exc_info=True)
            return None
        finally:
            cursor.close()
    
    def get_alerts(self, start_time, end_time, vehicle_id=None):
        """Retrieve all alerts within the specified time frame, optionally filtering by vehicle_id."""
        cursor = self.db_connection.cursor()
        try:
            if vehicle_id:
                query = sql.SQL("""
                    SELECT * FROM alerts
                    WHERE timestamp BETWEEN %s AND %s
                    AND vehicle_id = %s
                """)
                cursor.execute(query, (start_time, end_time, vehicle_id))
            else:
                query = sql.SQL("""
                    SELECT * FROM alerts
                    WHERE timestamp BETWEEN %s AND %s
                """)
                cursor.execute(query, (start_time, end_time))

            alerts = cursor.fetchall()
            return alerts
        except Exception as e:
            LOGGER.error("Failed to retrieve alerts", exc_info=True)
            return []
        finally:
            cursor.close()


    def create_alert(self, vehicle_id, location_type, alert_time):
        """Create a new alert for a vehicle."""
        cursor = self.db_connection.cursor()
        try:
            query = sql.SQL("""
                INSERT INTO alerts (vehicle_id, timestamp, location_type)
                VALUES (%s, %s, %s)
            """)
            cursor.execute(query, (vehicle_id, alert_time, location_type))
            self.db_connection.commit()
            LOGGER.info(f"Alert created for vehicle_id: {vehicle_id}, location_type: {location_type}")
        except Exception as e:
            self.db_connection.rollback()
            LOGGER.error("Failed to create alert", exc_info=True)
        finally:
            cursor.close()
    
    def insert_event(self, timestamp, is_driving_safe, vehicle_id, location_type):
        """Insert an event into the events table."""
        cursor = None
        try:
            query = sql.SQL("""
                INSERT INTO events (timestamp, is_driving_safe, vehicle_id, location_type)
                VALUES (%s, %s, %s, %s)
            """)
            cursor = self.db_connection.cursor()
            cursor.execute(query, (timestamp, is_driving_safe, vehicle_id, location_type))
            self.db_connection.commit()
            LOGGER.info(f"Event inserted: {timestamp}, {is_driving_safe}, {vehicle_id}, {location_type}")
        except Exception as e:
            self.db_connection.rollback()
            LOGGER.error("Failed to insert event", exc_info=True)
        finally:
            if cursor:
                cursor.close()

    def check_and_create_tables(self, sql_file_path):
            """Check if tables exist, and create them if they don't."""
            cursor = None
            try:
                cursor = self.db_connection.cursor()

                # Check if 'events' table exists
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = 'events'
                    );
                """)
                events_exists = cursor.fetchone()[0]

                # Check if 'alerts' table exists
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = 'alerts'
                    );
                """)
                alerts_exists = cursor.fetchone()[0]

                if events_exists and alerts_exists:
                    LOGGER.info("Tables 'events' and 'alerts' already exist.")
                else:
                    LOGGER.info("Creating missing tables...")
                    self.execute_sql_file(sql_file_path)
                    LOGGER.info("Tables created successfully.")

            except Exception as e:
                LOGGER.error("Failed to check or create tables", exc_info=True)
                self.db_connection.rollback()
            finally:
                if cursor:
                    cursor.close()