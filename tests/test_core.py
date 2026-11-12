import json
from pathlib import Path
import tempfile
import unittest

from collector.domain import collect
from collector.storage import Store
from collector.transport import Client, replay_server


class StorageTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.temporary.name) / 'store.sqlite3')

    def tearDown(self):
        self.store.close()
        self.temporary.cleanup()

    def test_duplicate_import_is_idempotent(self):
        for unused in range(2):
            self.store.ingest('来源甲', b'payload', [dict(key='a', value=1)])
        self.assertEqual(len(self.store.records()), 1)
        self.assertEqual(self.store.connection.execute('SELECT count(*) FROM imports').fetchone()[0], 1)

    def test_failed_batch_rolls_back(self):
        with self.assertRaises(ValueError):
            self.store.ingest('来源甲', b'bad', [dict(key='a'), dict(value=2)])
        self.assertEqual(self.store.records(), [])
        self.assertEqual(self.store.connection.execute('SELECT count(*) FROM raw_objects').fetchone()[0], 0)

    def test_source_evidence(self):
        digest = self.store.ingest('来源乙', b'body', [dict(key='a')])
        self.assertEqual(self.store.provenance('a'), dict(source='来源乙', digest=digest))
        self.assertIsNone(self.store.provenance('missing'))

    def test_stale_revision_does_not_overwrite(self):
        self.store.ingest('来源', b'1', [dict(key='a', revision=2, value='新')])
        self.store.ingest('来源', b'2', [dict(key='a', revision=1, value='旧')])
        self.assertEqual(self.store.records()[0]['value'], '新')

    def test_equal_revision_return_is_digested(self):
        self.store.ingest('来源', b'1', [dict(key='a', revision=2, value='甲')])
        self.store.ingest('来源', b'2', [dict(key='a', revision=2, value='乙')])
        self.assertEqual(self.store.records()[0]['value'], '甲')

    def test_newer_revision_overwrites(self):
        self.store.ingest('来源', b'1', [dict(key='a', revision=1, value='旧')])
        self.store.ingest('来源', b'2', [dict(key='a', revision=2, value='新')])
        self.assertEqual(self.store.records()[0]['value'], '新')

    def test_record_without_revision_keeps_legacy_replace(self):
        self.store.ingest('来源', b'1', [dict(key='a', value=1)])
        self.store.ingest('来源', b'2', [dict(key='a', revision=1, value='带修订')])
        self.assertEqual(self.store.records()[0]['value'], '带修订')
        self.store.ingest('来源', b'3', [dict(key='a', value=2)])
        self.assertEqual(self.store.records()[0]['value'], 2)

    def test_non_integer_revision_rejected_and_rolled_back(self):
        with self.assertRaises(ValueError):
            self.store.ingest('来源', b'bad', [dict(key='a', revision='3', value='x')])
        self.assertEqual(self.store.records(), [])
        self.assertEqual(self.store.connection.execute('SELECT count(*) FROM raw_objects').fetchone()[0], 0)

    def test_reopen_persists(self):
        self.store.ingest('来源', b'raw', [dict(key='p')])
        path = Path(self.temporary.name) / 'store.sqlite3'
        second = Store(path)
        try:
            self.assertEqual(second.records()[0]['key'], 'p')
        finally:
            second.close()


class TransportTests(unittest.TestCase):
    def test_scripted_responses_and_requests(self):
        with replay_server({'/x': [dict(status=429, headers={'Retry-After': '2'}), dict(body='收到')]}) as (base, calls):
            self.assertEqual(Client().request(base + '/x')[0], 429)
            self.assertEqual(Client().request(base + '/x')[2].decode(), '收到')
            self.assertEqual(Client().request(base + '/missing')[0], 404)
            self.assertEqual(len(calls), 3)

    def test_post_body(self):
        with replay_server({'/x': [dict(body='完成')]}) as (base, calls):
            Client().request(base + '/x', method='POST', body=b'a=1')
            self.assertEqual(calls[0]['body'], 'a=1')


class ImportTests(unittest.TestCase):
    def test_local_sample_import(self):
        raw = Path('fixtures/sample.json').read_bytes()
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory) / 'sample.sqlite3')
            try:
                records = collect(json.loads(raw))
                self.assertTrue(records)
                self.assertEqual(len({row['key'] for row in records}), len(records))
                store.ingest('样本来源', raw, records)
                self.assertEqual(len(records), len(store.records()))
            finally:
                store.close()
