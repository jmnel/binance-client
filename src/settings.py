from pathlib import Path

ENDPOINT_BASE = 'https://api.binance.com/api/v3/'
DATA_DIRECTORY = Path(__file__).resolve().parents[1] / 'data'
DATABASE_NAME = 'binance.sqlite3'
WEIGHT_DECAY = 15.0

NUM_WORKERS = 16
