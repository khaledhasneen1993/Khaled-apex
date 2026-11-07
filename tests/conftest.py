"""All tests are offline and use scoped Paper configuration by default."""
import socket
import pytest
from risk.config import execution_mode


@pytest.fixture(autouse=True)
def isolated_offline_test(monkeypatch):
    for name in ('BINANCE_API_KEY', 'BINANCE_API_SECRET', 'BINANCE_RSA_PRIVATE_KEY_PATH',
                 'TELEGRAM_BOT_TOKEN', 'TELEGRAM_CHAT_ID', 'CASCADE_DASHBOARD_TOKEN'):
        monkeypatch.delenv(name, raising=False)
    original = socket.socket.connect
    def connect(sock, address):
        if sock.family in (socket.AF_INET, socket.AF_INET6):
            raise AssertionError('Network access forbidden in tests; use a transport mock')
        return original(sock, address)
    monkeypatch.setattr(socket.socket, 'connect', connect)
    monkeypatch.setattr(socket.socket, 'connect_ex', connect)
    with execution_mode('paper'):
        yield
