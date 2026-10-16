import sqlite3
import unittest

from google_010_activity import read_activity


class GoogleActivityTest(unittest.TestCase):
    def test_counts_all_organizations_and_keeps_unknown_acceptance_busy(self):
        with sqlite3.connect(':memory:') as db:
            db.execute('CREATE TABLE channel_calls (organization_id TEXT, state TEXT)')
            db.executemany('INSERT INTO channel_calls VALUES (?,?)', [
                ('one', 'completed'), ('two', 'acceptance_unknown'),
                ('two', 'queued'), ('one', 'rejected'), ('one', 'failed')])
            db.commit()
            db.execute('PRAGMA query_only=ON')
            self.assertEqual(read_activity(db), {'schema_version': 1, 'scope': 'installation',
                                                'active_count': 2, 'submitted_count': 5})
            with self.assertRaises(sqlite3.OperationalError):
                db.execute('DELETE FROM channel_calls')

    def test_unknown_state_or_missing_schema_fails_closed(self):
        with sqlite3.connect(':memory:') as db:
            with self.assertRaises(sqlite3.OperationalError):
                read_activity(db)
            db.rollback()
            db.execute('CREATE TABLE channel_calls (state TEXT)')
            db.execute("INSERT INTO channel_calls VALUES ('new_state')")
            db.commit()
            with self.assertRaises(ValueError):
                read_activity(db)

    def test_empty_database_has_no_active_tasks(self):
        with sqlite3.connect(':memory:') as db:
            db.execute('CREATE TABLE channel_calls (state TEXT)')
            self.assertEqual(read_activity(db)['active_count'], 0)


if __name__ == '__main__':
    unittest.main()
