"""Fixed Runtime adapter for the published ComfyUI 0.1.3 image only.

Executed by authenticated Kubernetes exec, never an App-supplied command. No
configuration, database location or code is accepted from stdin.
"""
import contextlib
import json
import os
from pathlib import Path
import signal
import sqlite3
import stat
import sys


def fail():
    raise ValueError('LEGACY_MAINTENANCE_UNAVAILABLE')


def pairs(items):
    result = {}
    for key, value in items:
        if key in result:
            fail()
        result[key] = value
    return result


def read(path, maximum):
    with Path(path).open('rb') as stream:
        raw = stream.read(maximum + 1)
    if len(raw) > maximum:
        fail()
    return raw


def process():
    # Re-read after the SQLite transaction as well. The Runtime separately
    # verifies Pod UID, container ID and restart count before and after exec.
    if (read('/proc/1/cmdline', 4096).split(b'\0') !=
            [b'python', b'-m', b'app.server', b'--config', b'/config/app.json', b'']
            or Path('/proc/1/cwd').resolve() != Path('/opt/vf')
            or Path('/proc/1/exe').resolve() != Path('/proc/self/exe').resolve()):
        fail()
    return read('/proc/1/stat', 4096).split(b') ', 1)[1].split()[19]


def main():
    signal.signal(signal.SIGALRM, lambda *_: fail())
    signal.alarm(8)
    raw = sys.stdin.buffer.read(16385)
    if len(raw) > 16384:
        fail()
    request = json.loads(raw, object_pairs_hook=pairs)
    if (not isinstance(request, dict) or set(request) != {'action', 'proof'}
            or request['action'] not in ('seal', 'status', 'release', 'cancel')):
        fail()
    proof = request['proof']
    fields = {'schema_version', 'operation_id', 'installation_id', 'station_id',
              'organization_id', 'app_version', 'data_contract', 'dashboard_digest'}
    if (not isinstance(proof, dict) or set(proof) != fields
            or type(proof['schema_version']) is not int or proof['schema_version'] != 1
            or proof['app_version'] != '0.1.3' or proof['data_contract'] != 'comfyui-management-v1'):
        fail()
    before = process()
    # -I discards ambient Python paths. Import only this verified image's code.
    sys.path.insert(0, '/opt/vf')
    from app.protocol import decode, valid_id
    from app.compatibility import WorkerCompatibility
    from app.dashboard import DashboardAssets
    from app.management_config import resolve_management_settings
    from app.maintenance import MaintenanceAdmission
    from app.task_store import TaskError, TaskStore

    if not all(valid_id(proof[key]) for key in ('operation_id', 'installation_id', 'station_id', 'organization_id')):
        fail()
    if read('/opt/vf/VERSION', 64).decode().strip() != '0.1.3':
        fail()
    compatibility = WorkerCompatibility.load('/opt/vf/management-release.json', '0.1.3')
    if compatibility.default_worker_version != '0.1.3' or compatibility.worker_versions != ('0.1.3',):
        fail()
    declaration = DashboardAssets('/opt/vf/build/dashboard').declaration()
    if declaration['appVersion'] != '0.1.3' or declaration['dashboard']['artifactDigest'] != proof['dashboard_digest']:
        fail()
    environment = {}
    for entry in read('/proc/1/environ', 1 << 20).split(b'\0'):
        if not entry:
            continue
        key, value = entry.split(b'=', 1)
        if key in (b'COMFYUI_INSTALLATION_ID', b'COMFYUI_STATION_ID', b'COMFYUI_APP_VERSION'):
            name = key.decode()
            if name in environment:
                fail()
            environment[name] = value.decode()
    config = decode(read('/config/app.json', 65536))
    settings = resolve_management_settings(config['management'], environment, release_version='0.1.3')
    if (settings['installation_id'] != proof['installation_id'] or settings['station_id'] != proof['station_id']
            or config['database'] != '/data/app.sqlite3' or config.get('worker_origin')):
        fail()
    filename = Path('/data/app.sqlite3')
    info = filename.lstat()
    if not stat.S_ISREG(info.st_mode) or filename.resolve() != filename:
        fail()
    inode = (info.st_dev, info.st_ino)

    class ExistingStore(TaskStore):
        def __init__(self):
            # Do not call TaskStore.__init__: it creates and migrates databases.
            self.filename = str(filename)

        @contextlib.contextmanager
        def transaction(self):
            db = sqlite3.connect('file:/data/app.sqlite3?mode=rw', uri=True, timeout=5, isolation_level=None)
            try:
                db.row_factory = sqlite3.Row
                db.execute('PRAGMA foreign_keys=ON')
                db.execute('PRAGMA synchronous=FULL')
                db.execute('BEGIN IMMEDIATE')
                current = filename.lstat()
                if (current.st_dev, current.st_ino) != inode or filename.resolve() != filename:
                    fail()
                yield db
                if process() != before:
                    fail()
                current = filename.lstat()
                if (current.st_dev, current.st_ino) != inode or filename.resolve() != filename:
                    fail()
                db.commit()
            except BaseException:
                db.rollback()
                raise
            finally:
                db.close()

    class CancelableAdmission(MaintenanceAdmission):
        def cancel(self, installation_id, station_id, organization_id, operation_id):
            """Persist a tombstone even when seal has not arrived yet."""
            scope = self._scope(installation_id, station_id, organization_id, operation_id)
            with self.store.transaction() as db:
                row = self._prior(db, scope)
                if row is None:
                    rows = db.execute('''SELECT m.station_id,i.organization_id FROM managed_instances m
                        JOIN instances i ON i.id=m.id WHERE m.installation_id=?''', (installation_id,)).fetchall()
                    if any((r['station_id'], r['organization_id']) != (station_id, organization_id) for r in rows):
                        raise TaskError('MAINTENANCE_SCOPE_CONFLICT')
                    db.execute("INSERT INTO maintenance_seals VALUES (?,?,?,?,'released')",
                               (operation_id, installation_id, station_id, organization_id))
                else:
                    db.execute("UPDATE maintenance_seals SET state='released' WHERE operation_id=?", (operation_id,))
                return self._view(self._prior(db, scope))

    gate = CancelableAdmission(ExistingStore())
    try:
        row = getattr(gate, request['action'])(proof['installation_id'], proof['station_id'],
                                               proof['organization_id'], proof['operation_id'])
    except TaskError as error:
        code = str(error)
        if code not in ('INSTALLATION_BUSY', 'MAINTENANCE_NOT_FOUND', 'MAINTENANCE_CONFLICT', 'MAINTENANCE_SCOPE_CONFLICT'):
            fail()
        return {'code': code}
    if process() != before or row['state'] not in ('sealed', 'released'):
        fail()
    return {'proof': {**proof, 'maintenance_state': row['state']}}


if __name__ == '__main__':
    try:
        result = main()
    except BaseException:
        result = {'code': 'LEGACY_MAINTENANCE_UNAVAILABLE'}
    sys.stdout.write(json.dumps(result, separators=(',', ':')))
