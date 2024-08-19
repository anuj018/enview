# Load ENV from the .env
from dotenv import load_dotenv

load_dotenv()

import os

config = {
    "postgres": {
        "url": os.getenv("POSTGRES_URL", ""),
        "port": int(os.getenv("POSTGRES_PORT", "-1")),
        "database": os.getenv("POSTGRES_DB", ""),
        "username": os.getenv("POSTGRES_USERNAME", ""),
        "password": os.getenv("POSTGRES_PASSWORD", ""),
    },
    # Valid location types and thresholds
    "location_thresholds" : {
        "highway": 4,
        "city_center": 3,
        "commercial": 2,
        "residential": 1
    },
    #valid locations (non exhaustive)
    "valid_location_types" : {"highway", "city_center", "commercial", "residential"}
}
