"""Read-only installation occupancy for the historical Video 0.17.28 image.

No app imports: importing the service can initialize storage or resume tasks.
Pod identity and request gating are responsibilities of the Runtime caller.
"""
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import re
import stat
import time


def unique(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('unavailable')
        result[key] = value
    return result


def invalid_constant(_):
    raise ValueError('unavailable')


def busy(record, task_id):
    if (not isinstance(record, dict) or
            type(record.get('schema_version')) is not int or record['schema_version'] != 1 or
            record.get('video_task_id') != task_id or
            record.get('status') not in {'queued', 'running', 'succeeded', 'failed', 'cancelled'} or
            not isinstance(record.get('request'), dict)):
        raise ValueError('unavailable')
    error, delivery = record.get('error'), record.get('delivery')
    if error is not None and not isinstance(error, dict):
        raise ValueError('unavailable')
    if delivery is not None and (not isinstance(delivery, dict) or
            delivery.get('status') not in {'queued', 'running', 'succeeded', 'failed'}):
        raise ValueError('unavailable')
    return (record['status'] in {'queued', 'running'} or
            (error or {}).get('code') == 'submission_unconfirmed' or
            record.get('runtime_stage') == 'submission_unknown' or
            bool(delivery and delivery['status'] in {'queued', 'running'}) or
            bool(record['request'].get('postprocess') and
                 (delivery or {}).get('status') not in {'succeeded', 'failed'}))


def identity(value):
    return (value.st_dev, value.st_ino, value.st_mode, value.st_size,
            value.st_mtime_ns, value.st_ctime_ns)


def scan(root, deadline):
    before = root.lstat()
    if not stat.S_ISDIR(before.st_mode):
        raise ValueError('unavailable')
    evidence, active, size = {}, 0, 0
    for path in sorted(root.iterdir()):
        if time.monotonic() >= deadline or path.name.startswith('.task-'):
            raise ValueError('unavailable')
        if not path.name.startswith('video_task_') or not path.name.endswith('.json'):
            continue
        if not re.fullmatch(r'video_task_[0-9a-f]{32}\.json', path.name):
            raise ValueError('unavailable')
        first = path.lstat()
        if not stat.S_ISREG(first.st_mode) or first.st_size > 4 * 1024 * 1024:
            raise ValueError('unavailable')
        with path.open('rb') as stream:
            opened = os.fstat(stream.fileno())
            raw = stream.read(4 * 1024 * 1024 + 1)
            closed = os.fstat(stream.fileno())
        size += len(raw)
        # Compare ctime within each API: Windows stat/fstat may expose different
        # ctime meanings. Inode, size and mtime must also agree across APIs.
        if (size > 64 * 1024 * 1024 or len(raw) != first.st_size or
                identity(first)[:5] != identity(opened)[:5] or identity(opened) != identity(closed) or
                identity(first) != identity(path.lstat())):
            raise ValueError('unavailable')
        record = json.loads(raw, object_pairs_hook=unique, parse_constant=invalid_constant)
        active += busy(record, path.stem)
        evidence[path.name] = (identity(first), hashlib.sha256(raw).hexdigest())
    if identity(before) != identity(root.lstat()) or time.monotonic() >= deadline:
        raise ValueError('unavailable')
    return identity(before), evidence, active


def read_activity(root):
    deadline = time.monotonic() + 4
    before = scan(root, deadline)
    if before != scan(root, deadline):
        raise ValueError('unavailable')
    return {'schema_version': 1, 'scope': 'installation',
            'submitted_count': len(before[1]), 'active_count': before[2]}


def dashboard_digest(root):
    # Preserve the signed historical index order and bytes (HTML, JS, CSS).
    if root.is_symlink() or not root.is_dir():
        raise ValueError('unavailable')
    paths = list(root.rglob('*'))
    if any(p.is_symlink() for p in paths):
        raise ValueError('unavailable')
    files = [p for p in paths if p.is_file()]
    html = [p for p in files if p.relative_to(root).as_posix() == 'index.html']
    js = [p for p in files if p.parent == root / 'assets' and p.suffix == '.js']
    css = [p for p in files if p.parent == root / 'assets' and p.suffix == '.css']
    if len(files) != 3 or len(html) != 1 or len(js) != 1 or len(css) != 1:
        raise ValueError('unavailable')
    hashes = {}
    for path in html + js + css:
        if path.stat().st_size > 4 * 1024 * 1024:
            raise ValueError('unavailable')
        hashes[path.relative_to(root).as_posix()] = hashlib.sha256(path.read_bytes()).hexdigest()
    index = json.dumps({'schemaVersion': 1, 'files': hashes}, indent=2) + '\n'
    return 'sha256:' + hashlib.sha256(index.encode()).hexdigest()


def main():
    version = importlib.metadata.version('verdantflare-video-mcp-server')
    if version not in {'0.17.28', '0.17.29', '0.17.30', '0.17.31', '0.17.32', '0.17.35', '0.17.37', '0.17.38', '0.17.39'}:
        raise ValueError('unavailable')
    command = Path('/proc/1/cmdline').read_bytes().rstrip(b'\0').split(b'\0')
    if (len(command) != 10 or Path(os.fsdecode(command[0])).name not in {'python', 'python3', 'python3.11'} or
            command[1:] != [b'-m', b'uvicorn', b'app.server:app', b'--host', b'0.0.0.0',
                            b'--port', b'8000', b'--workers', b'1']):
        raise ValueError('unavailable')
    if os.environ.get('VIDEO_ARTIFACT_ROOT') != '/data/projects/video':
        raise ValueError('unavailable')
    result = read_activity(Path('/data/projects/video/tasks'))
    result.update(app_version=version, dashboard_digest=dashboard_digest(Path('/app/frontend/dist')))
    print(json.dumps(result))


if __name__ == '__main__':
    try:
        main()
    except Exception:
        print('{"error":"activity_unavailable"}')
