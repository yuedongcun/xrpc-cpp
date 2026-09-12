import json
import tempfile
import unittest
from pathlib import Path

from run_suite import load_config


class PoolConfigTest(unittest.TestCase):
    def load(self, **pool):
        data = {"type": "client", "duration": 1, "payload_size": 128,
                "server_worker_threads": 1, "server_connection_io_threads": 1,
                "threads": 1, **pool}
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.json"
            path.write_text(json.dumps(data))
            return load_config(path)

    def test_pool_parameters_are_optional_and_can_be_set_independently(self):
        default = self.load()
        self.assertEqual(default.server_recv_buffer_count, 0)
        self.assertEqual(default.server_recv_buffer_size, 0)
        self.assertEqual(self.load(server_recv_buffer_count=2).server_recv_buffer_count, 2)
        self.assertEqual(self.load(server_recv_buffer_size=256).server_recv_buffer_size, 256)

    def test_invalid_pool_parameters_are_rejected_before_launch(self):
        for key, value in [("server_recv_buffer_count", 0), ("server_recv_buffer_count", 3),
                           ("server_recv_buffer_count", 65536), ("server_recv_buffer_count", True),
                           ("server_recv_buffer_size", 0), ("server_recv_buffer_size", -1),
                           ("server_recv_buffer_size", 1 << 32)]:
            with self.subTest(key=key, value=value), self.assertRaises(RuntimeError):
                self.load(**{key: value})


if __name__ == "__main__":
    unittest.main()
