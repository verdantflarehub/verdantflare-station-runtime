"""Occupancy protection tests without services, containers or supplier calls."""
import contextlib
import io
import json
import hashlib
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import video_01728_activity as activity


class VideoActivityTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def add(self, number=1, **values):
        task_id = 'video_task_' + format(number, '032x')
        record = dict(schema_version=1, video_task_id=task_id, status='succeeded', request={})
        record.update(values)
        path = self.root / (task_id + '.json')
        path.write_text(json.dumps(record), encoding='utf-8')
        return path

    def test_all_tasks_are_read_without_mutation(self):
        (self.root / ('video_task_' + '0' * 32 + '.higgsfield.mp4')).write_bytes(b'media')
        for number in range(120):
            self.add(number, authority={'organization_id': str(number)},
                     status='running' if number == 119 else 'succeeded')
        before = {p.name: p.read_bytes() for p in self.root.iterdir()}
        self.assertEqual(activity.read_activity(self.root), {
            'schema_version': 1, 'scope': 'installation',
            'submitted_count': 120, 'active_count': 1})
        self.assertEqual(before, {p.name: p.read_bytes() for p in self.root.iterdir()})

    def test_unknown_submissions_and_postprocessing_block_stop(self):
        for values in [dict(status='queued'), dict(status='running'),
                       dict(status='failed', error={'code': 'submission_unconfirmed'}),
                       dict(status='failed', runtime_stage='submission_unknown'),
                       dict(request={'postprocess': {'interpolate': {}}}),
                       dict(delivery={'status': 'queued'}),
                       dict(delivery={'status': 'running'})]:
            with self.subTest(values=values):
                self.add(**values)
                self.assertEqual(activity.read_activity(self.root)['active_count'], 1)
        for status in ['succeeded', 'failed']:
            self.add(request={'postprocess': True}, delivery={'status': status})
            self.assertEqual(activity.read_activity(self.root)['active_count'], 0)

    def test_invalid_records_fail_closed(self):
        for values in [dict(status='new_status'), dict(video_task_id='other'),
                       dict(schema_version=True), dict(request=None),
                       dict(delivery={'status': 'unknown'}), dict(error='bad')]:
            with self.subTest(values=values):
                self.add(**values)
                with self.assertRaises(ValueError):
                    activity.read_activity(self.root)
        path = self.add()
        for raw in ['{', '{"status":"succeeded","status":"queued"}', '{"x":NaN}']:
            path.write_text(raw, encoding='utf-8')
            with self.assertRaises(ValueError):
                activity.read_activity(self.root)

    def test_missing_directory_partial_writes_and_invalid_names(self):
        with self.assertRaises(FileNotFoundError):
            activity.read_activity(self.root / 'missing')
        for name in ['.task-in-progress', 'video_task_broken.json']:
            path = self.root / name
            path.write_text('{}', encoding='utf-8')
            with self.assertRaises(ValueError):
                activity.read_activity(self.root)
            path.unlink()
        (self.root / ('video_task_' + '0' * 32 + '.json')).mkdir()
        with self.assertRaises(ValueError):
            activity.read_activity(self.root)

    def test_changes_between_scans_fail_closed(self):
        self.add()
        original = activity.scan
        calls = 0

        def changed(root, deadline):
            nonlocal calls
            calls += 1
            if calls == 2:
                self.add(2, status='running')
            return original(root, deadline)

        with patch.object(activity, 'scan', changed), self.assertRaises(ValueError):
            activity.read_activity(self.root)

    def test_entrypoint_requires_exact_version_process_and_root(self):
        command = b'\0'.join([b'python3', b'-m', b'uvicorn', b'app.server:app', b'--host',
                              b'0.0.0.0', b'--port', b'8000', b'--workers', b'1']) + b'\0'
        with patch.object(activity.importlib.metadata, 'version', return_value='0.17.28'), \
                patch.object(Path, 'read_bytes', return_value=command), \
                patch.dict(activity.os.environ, {'VIDEO_ARTIFACT_ROOT': '/data/projects/video'}), \
                patch.object(activity, 'dashboard_digest', return_value='sha256:test'), \
                patch.object(activity, 'read_activity', return_value={'active_count': 0}) as read:
            with contextlib.redirect_stdout(io.StringIO()):
                activity.main()
            read.assert_called_once_with(Path('/data/projects/video/tasks'))
            with patch.object(activity.importlib.metadata, 'version', return_value='0.17.27'):
                with self.assertRaises(ValueError):
                    activity.main()
            with patch.object(Path, 'read_bytes', return_value=command + b'extra'):
                with self.assertRaises(ValueError):
                    activity.main()
            with patch.dict(activity.os.environ, {'VIDEO_ARTIFACT_ROOT': '/other'}):
                with self.assertRaises(ValueError):
                    activity.main()

    def test_dashboard_matches_signed_index_and_refuses_extra_assets(self):
        (self.root / 'assets').mkdir()
        files = {'index.html': b'html', 'assets/main.js': b'js', 'assets/main.css': b'css'}
        for name, raw in files.items():
            (self.root / name).write_bytes(raw)
        index = json.dumps({'schemaVersion': 1, 'files': {
            name: hashlib.sha256(raw).hexdigest() for name, raw in files.items()}}, indent=2) + '\n'
        self.assertEqual(activity.dashboard_digest(self.root), 'sha256:' + hashlib.sha256(index.encode()).hexdigest())
        (self.root / 'assets/extra.js').write_bytes(b'extra')
        with self.assertRaises(ValueError):
            activity.dashboard_digest(self.root)


if __name__ == '__main__':
    unittest.main()
