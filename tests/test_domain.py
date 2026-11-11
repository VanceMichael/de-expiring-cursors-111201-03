import json
from pathlib import Path
import unittest

from collector.domain import *


class DomainTests(unittest.TestCase):
    def setUp(self):
        self.payload = json.loads(Path('fixtures/sample.json').read_text())

    def test_domain_behavior(self):
        source = Catalog(self.payload['records'], page_size=1)
        first = source.page(0, 10)
        self.assertTrue(first['next'])
        self.assertEqual(source.page(0, 10, cursor=first['next'])['items'][0]['id'], 'b')
        with self.assertRaises(CursorExpired):
            source.page(0, 10, clock=6, cursor=first['next'])

    def test_deterministic_observations(self):
        self.assertEqual(collect(self.payload), collect(self.payload))
