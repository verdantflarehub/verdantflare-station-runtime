"""Unit tests; no containers, server or supplier requests."""
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
import uuid
from contextlib import closing

from fal_010_activity import read_activity, unique


class LegacyActivityTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / 'state.sqlite3'
        self.db = sqlite3.connect(self.path)
        self.db.execute('CREATE TABLE submissions(id TEXT,organization_id TEXT,state TEXT,provider_request_id TEXT)')
        self.add()

    def tearDown(self):
        self.db.close()
        self.temp.cleanup()

    def add(self, state='queued', remote='remote', org='org'):
        sid = str(uuid.uuid4())
        self.db.execute('INSERT INTO submissions VALUES (?,?,?,?)', (sid, org, state, remote))
        self.db.commit()
        return sid

    def completed(self, sid, org):
        return {'id': sid, 'state': 'completed'}

    def test_complete_is_read_only_and_unknown_other_org_busy(self):
        before = '\n'.join(self.db.iterdump())
        self.assertEqual(read_activity(self.db, self.completed)['active_count'], 0)
        self.assertEqual(before, '\n'.join(self.db.iterdump()))
        self.add('submission_unconfirmed', None, 'other-org')
        self.assertEqual(read_activity(self.db, self.completed)['active_count'], 1)

    def test_limits_and_unsupported_status_fail_closed(self):
        for _ in range(16):
            self.add()
        with self.assertRaises(ValueError):
            read_activity(self.db, self.completed)
        self.db.execute("UPDATE submissions SET state='invalid'")
        self.db.commit()
        with self.assertRaises(ValueError):
            read_activity(self.db, self.completed)

    def test_wrong_identity_unknown_status_and_concurrent_change(self):
        for value in [{'id': 'wrong', 'state': 'completed'}, {'state': 'invalid'}]:
            with self.assertRaises(ValueError):
                read_activity(self.db, lambda *_: value)

        def changed(sid, org):
            with closing(sqlite3.connect(self.path)) as writer:
                writer.execute('INSERT INTO submissions VALUES (?,?,?,?)',
                               (str(uuid.uuid4()), 'other', 'submission_unconfirmed', None))
                writer.commit()
            return self.completed(sid, org)

        with self.assertRaises(ValueError):
            read_activity(self.db, changed)

    def test_duplicate_keys_refused(self):
        with self.assertRaises(ValueError):
            json.loads('{"state":"completed","state":"in_progress"}', object_pairs_hook=unique)


if __name__ == '__main__':
    unittest.main()
