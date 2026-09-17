"""Exercise release guards without publishing or contacting a registry."""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]


class ReleaseGuards(unittest.TestCase):
    def test_registry_errors_fail_closed(self):
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            docker = root/'docker'
            docker.write_text('#!/bin/sh\nprintf "%s\\n" "$MOCK_OUTPUT"\nexit "$MOCK_STATUS"\n')
            docker.chmod(0o700)
            for status, output, accepted in [
                (0, 'existing manifest', False),
                (1, 'manifest unknown', True),
                (1, 'ERROR: registry.invalid/repo:tag: not found', True),
                (1, 'manifest registry.invalid/repo:tag: not found', True),
                (1, 'unauthorized', False),
                (1, 'repository not found', False),
                (1, 'connection timeout', False),
                (1, 'unexpected error', False),
            ]:
                with self.subTest(output=output):
                    env = dict(os.environ, PATH=str(root)+os.pathsep+os.environ['PATH'],
                               IMAGE='registry.invalid/repo:tag', MOCK_STATUS=str(status), MOCK_OUTPUT=output)
                    result = subprocess.run(['bash', str(ROOT/'scripts/check-image-absent.sh')],
                                            env=env, capture_output=True)
                    self.assertEqual(result.returncode == 0, accepted)


if __name__ == '__main__':
    unittest.main()
