import fcntl
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import sessions

class SessionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)/'paper-dashboard'
        self.patch = patch.object(sessions, 'BASE', self.base)
        self.patch.start(); self.addCleanup(self.patch.stop)

    def test_new_and_resumed_sessions_keep_old_accounts(self):
        first = sessions.select_session()
        ledger = first/'accounts/khaled/state.json'; ledger.parent.mkdir(parents=True)
        ledger.write_text('{"balance_usdt": 77, "trades": ["old"]}')
        self.assertEqual(sessions.select_session(), first)
        second = sessions.select_session(fresh=True)
        self.assertNotEqual(first, second)
        self.assertFalse((second/'accounts/khaled/state.json').exists())
        self.assertEqual(json.loads(ledger.read_text())['balance_usdt'], 77)
        self.assertEqual(json.loads((second/'session.json').read_text()),dict(
            mode='paper',initial_balance_usdt=100,bots=['final','khaled'],session=second.name))
        self.assertEqual(sessions.select_session(), second)

    def test_running_server_blocks_session_change(self):
        sessions.select_session()
        before=(self.base/'current').read_bytes()
        with (self.base/'server.lock').open('a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.assertRaises(ValueError): sessions.select_session(fresh=True)
        self.assertEqual((self.base/'current').read_bytes(), before)

    def test_cannot_resume_demo_or_arbitrary_directory(self):
        self.base.mkdir()
        (self.base/'current').write_text(str(Path(self.temp.name)))
        with self.assertRaises(ValueError): sessions.select_session()
