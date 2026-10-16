"""Fixed, read-only occupancy adapter for Google 0.1.0 and 0.1.1."""
import importlib.metadata
import json
import os
from pathlib import Path
import sqlite3


def read_activity(db):
    db.execute('BEGIN')
    rows = db.execute('SELECT state,COUNT(*) FROM channel_calls GROUP BY state').fetchall()
    terminal = {'completed', 'failed', 'rejected'}
    active = {'acceptance_unknown', 'queued', 'in_queue', 'in_progress'}
    if any(state not in terminal | active for state, _ in rows):
        raise ValueError('unavailable')
    return {'schema_version': 1, 'scope': 'installation',
            'submitted_count': sum(count for _, count in rows),
            'active_count': sum(count for state, count in rows if state in active)}


def main():
    if importlib.metadata.version('verdantflare-app-google') not in {'0.1.0', '0.1.1'}:
        raise ValueError('unavailable')
    command = Path('/proc/1/cmdline').read_bytes().rstrip(b'\0').split(b'\0')
    if len(command) != 3 or Path(os.fsdecode(command[0])).name not in {'python', 'python3', 'python3.12'} or command[1:] != [b'-m', b'google_channel.server']:
        raise ValueError('unavailable')
    if os.environ.get('GOOGLE_STATE_DB') != '/data/google/state.sqlite3':
        raise ValueError('unavailable')
    with sqlite3.connect('file:/data/google/state.sqlite3?mode=ro', uri=True, timeout=1) as db:
        db.execute('PRAGMA query_only=ON')
        result = read_activity(db)
    print(json.dumps(result))


if __name__ == '__main__':
    try:
        main()
    except Exception:
        print('{"error":"activity_unavailable"}')
