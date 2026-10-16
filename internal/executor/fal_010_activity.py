"""Fixed, read-only occupancy adapter for the original fal 0.1.0 image."""
import importlib.metadata
import json
import os
from pathlib import Path
import re
import sqlite3
import time
import urllib.request


def read_activity(db, fetch):
    start = time.monotonic()
    before = db.execute('PRAGMA data_version').fetchone()[0]
    total = db.execute('SELECT COUNT(*) FROM submissions').fetchone()[0]
    rows = db.execute("SELECT id,organization_id,state,provider_request_id FROM submissions WHERE state!='rejected' LIMIT 17").fetchall()
    if len(rows) > 16:
        raise ValueError('unavailable')
    active = 0
    for sid, org, state, remote in rows:
        if time.monotonic() - start >= 4 or state not in {'queued', 'submission_unconfirmed'}:
            raise ValueError('unavailable')
        if not remote:
            active += 1
            continue
        if not re.fullmatch(r'[0-9a-f-]{36}', sid) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._:-]{0,127}', org):
            raise ValueError('unavailable')
        value = fetch(sid, org)
        if value.get('id') != sid or value.get('state') not in {'completed', 'in_queue', 'in_progress'}:
            raise ValueError('unavailable')
        active += value['state'] != 'completed'
    if time.monotonic() - start >= 4 or db.execute('PRAGMA data_version').fetchone()[0] != before:
        raise ValueError('unavailable')
    return {'schema_version': 1, 'scope': 'installation', 'submitted_count': total, 'active_count': active}


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        raise ValueError('unavailable')


def unique(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('unavailable')
        result[key] = value
    return result


def main():
    if importlib.metadata.version('verdantflare-video-fal-gateway') != '0.1.0':
        raise ValueError('unavailable')
    command = Path('/proc/1/cmdline').read_bytes().rstrip(b'\0').split(b'\0')
    if len(command) != 3 or Path(os.fsdecode(command[0])).name not in {'python', 'python3', 'python3.12'} or command[1:] != [b'-m', b'fal_gateway.server']:
        raise ValueError('unavailable')
    if os.environ.get('FAL_STATE_DB') != '/data/fal/state.sqlite3':
        raise ValueError('unavailable')
    token = os.environ['FAL_EXECUTION_TOKEN']
    if len(token) < 32:
        raise ValueError('unavailable')
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())

    def fetch(sid, org):
        req = urllib.request.Request('http://127.0.0.1:8001/v1/submissions/' + sid,
            headers={'Authorization': 'Bearer ' + token, 'X-Organization-Id': org})
        with opener.open(req, timeout=2) as response:
            raw = response.read(65537)
            if response.status != 200 or len(raw) > 65536:
                raise ValueError('unavailable')
        return json.loads(raw, object_pairs_hook=unique)

    with sqlite3.connect('file:/data/fal/state.sqlite3?mode=ro', uri=True, timeout=1) as db:
        db.execute('PRAGMA query_only=ON')
        result = read_activity(db, fetch)
    print(json.dumps(result))


if __name__ == '__main__':
    try:
        main()
    except Exception:
        # Only a fixed failure marker leaves the Pod, never exception details.
        print('{"error":"activity_unavailable"}')
