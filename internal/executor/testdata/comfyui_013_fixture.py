"""Isolated old-image test fixture; never shipped as a production exec adapter."""
import json
from pathlib import Path
import sys
from types import SimpleNamespace
import uuid

from app.editor import Editor, EMPTY_GRAPH
from app.management import InstanceManager
from app.protocol import Error, request_id
from app.service import Application
from app.task_store import Principal, TaskStore
from app.workflow import WorkflowPolicy

action = sys.argv[1] if len(sys.argv) > 1 else 'seed'
root = Path('/data')
filename = root / 'app.sqlite3'
if action == 'hide-database':
    filename.rename(root / 'saved.sqlite3')
elif action == 'restore-database':
    assert not filename.exists(), 'bootstrap created a missing database'
    (root / 'saved.sqlite3').rename(filename)
else:
    setup = json.loads(Path('/config/proof.json').read_text())
    proof = setup['proof']
    actor = Principal(proof['organization_id'], setup['user_id'])
    authority = SimpleNamespace(installation_id=proof['installation_id'], station_id=proof['station_id'],
                                app_version='0.1.3', check=lambda _: None)
    store = TaskStore(filename)
    manager = InstanceManager(store, authority)
    if action == 'seed':
        instance = manager.create(actor, {'idempotency_key': request_id(), 'name': 'Isolated maintenance fixture',
                                          'profile_id': 'base-cpu'})['instance_id']
        state = {'instance': instance, 'boot': str(uuid.uuid4())}
        (root / 'fixture.json').write_text(json.dumps(state))
    else:
        state = json.loads((root / 'fixture.json').read_text())
        app = Application(store, alias='comfyuiA', instance_id=state['instance'], principal=actor,
                          project_access=SimpleNamespace(), policy=WorkflowPolicy())
        editor = Editor(app, SimpleNamespace(identity=lambda _: state['boot']), Path('/opt/vf/build/frontend'))
        if action == 'busy':
            opened = editor.open(actor, str(uuid.uuid4()))
            state['session'] = opened['session_id']
            (root / 'fixture.json').write_text(json.dumps(state))
        elif action == 'save-close':
            graph = {**EMPTY_GRAPH, 'nodes': [{'id': 1, 'type': 'Note'}]}
            editor.update(actor, state['session'], 0, graph)
            editor.close(actor, state['session'])
            with store.transaction() as db:
                state['retained'] = {
                    'instance': dict(db.execute('SELECT * FROM instances WHERE id=?', (state['instance'],)).fetchone()),
                    'managed': dict(db.execute('SELECT * FROM managed_instances WHERE id=?', (state['instance'],)).fetchone()),
                    'draft': dict(db.execute('SELECT graph,version FROM base_editor WHERE instance_id=?', (state['instance'],)).fetchone())}
            (root / 'fixture.json').write_text(json.dumps(state))
        elif action == 'assert-sealed':
            try:
                editor.open(actor, str(uuid.uuid4()))
            except Error as error:
                assert error.code == 'INSTALLATION_MAINTENANCE'
            else:
                raise AssertionError('new editing admitted while sealed')
            with store.transaction() as db:
                assert dict(db.execute('SELECT * FROM instances WHERE id=?', (state['instance'],)).fetchone()) == state['retained']['instance']
                assert dict(db.execute('SELECT * FROM managed_instances WHERE id=?', (state['instance'],)).fetchone()) == state['retained']['managed']
                assert dict(db.execute('SELECT graph,version FROM base_editor WHERE instance_id=?', (state['instance'],)).fetchone()) == state['retained']['draft']
        elif action == 'assert-no-seals':
            with store.transaction() as db:
                assert db.execute('SELECT count(*) FROM maintenance_seals').fetchone()[0] == 0
        else:
            raise AssertionError('unknown fixture action')
print('FIXTURE_OK')
