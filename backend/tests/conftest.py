import os
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

DB_PATH = pathlib.Path('/tmp/replica_route_test.db')
if DB_PATH.exists():
    DB_PATH.unlink()
os.environ['DATABASE_URL'] = 'sqlite:///' + str(DB_PATH)
os.environ['SEED_DEMO'] = 'false'
os.environ['REDIS_URL'] = 'redis://127.0.0.1:6399/0'
os.environ.setdefault('JWT_SECRET', 'test-secret')
