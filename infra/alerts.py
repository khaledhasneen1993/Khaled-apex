import asyncio
import logging
import os
import httpx

from infra.http_logging import secure_http_logging


class TelegramAlerter:
    def __init__(self, cfg):
        secure_http_logging()
        self.cfg = cfg
        self.token = os.getenv('TELEGRAM_BOT_TOKEN', '')
        self.chat_id = os.getenv('TELEGRAM_CHAT_ID', '')
        self.enabled = bool(cfg['telegram_enabled'] and self.token and self.chat_id)
        self.queue = asyncio.Queue(maxsize=cfg['queue_size'])
        self.failed = False

    def send_nowait(self, msg, level='info'):
        """Trading/flattening must never wait behind a full notification queue."""
        if not self.enabled: return False
        text = f'[{level}] {msg}'
        limit = self.cfg['message_max_chars']
        try:
            for offset in range(0, len(text), limit):
                self.queue.put_nowait(text[offset:offset + limit])
        except asyncio.QueueFull:
            self.failed = True
            return False
        return True

    async def send(self, msg: str, level='info'):
        if not self.enabled:
            return False
        text = f'[{level}] {msg}'
        limit = self.cfg['message_max_chars']
        for offset in range(0, len(text), limit):
            await self.queue.put(text[offset:offset + limit])
        return True

    async def _worker(self):
        if not self.enabled:
            await asyncio.Future()  # cancellable; no HTTP client or network
            return
        async with httpx.AsyncClient(timeout=self.cfg['http_timeout_sec']) as client:
            while True:
                msg = await self.queue.get()
                delivered = False
                try:
                    for _ in range(self.cfg['max_attempts']):
                        try:
                            response = await client.post(
                                f"{self.cfg['base_url']}/bot{self.token}/sendMessage",
                                json={'chat_id': self.chat_id, 'text': msg})
                            delivered = response.status_code == 200 and response.json().get('ok') is True
                        except Exception:
                            delivered = False
                        if delivered:
                            break
                        await asyncio.sleep(self.cfg['retry_sec'])
                    self.failed = not delivered
                    if not delivered:
                        logging.getLogger('CASCADE').error('Telegram delivery failed; see durable operational journal')
                finally:
                    self.queue.task_done()
