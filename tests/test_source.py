import json
import unittest
from pathlib import Path
from reader import inspect
from lab import serving, get_json
from source import Source


class SourceTests(unittest.TestCase):
    def setUp(self):
        self.fixture = json.loads((Path(__file__).parents[1] / "fixtures/source.json").read_text(encoding="utf-8"))
        self.source = Source(self.fixture)

    def test_old_cursor_reports_gap(self):
        status, headers, body=Source(self.fixture).handle('/events', {'Last-Event-ID':'5'})
        self.assertEqual(status,409)
        self.assertEqual(json.loads(body)['snapshot'],'/snapshot')

    def test_chunks_preserve_bytes(self):
        from source import byte_chunks
        raw=b''.join(byte_chunks(self.fixture,1))
        self.assertIn('在线',raw.decode())
        self.assertFalse(raw.endswith(b'\n\n'))

    def test_snapshot_cut(self):
        with serving(Source(self.fixture)) as base:
            self.assertEqual(get_json(base+'/snapshot')['cut'],40)

    def test_chunk_limit(self):
        from source import byte_chunks
        with self.assertRaises(ValueError):byte_chunks(self.fixture,0)


if __name__ == "__main__":
    unittest.main()
