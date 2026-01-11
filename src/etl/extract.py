import json
import logging

import aiohttp

log = logging.getLogger(__name__)


class ResponseWrapper:
    def __init__(self, text: str, content: bytes):
        self.text = text
        self.content = content

    def json(self):
        if self.text is None:
            return self.text

        return json.loads(self.text)


class Extract:
    def __init__(self):
        self.session: aiohttp.ClientSession | None = None

    async def _ensure_session(self):
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()

    async def request_get(self, url: str, get_text: bool = True, **kwargs):
        await self._ensure_session()
        async with self.session.get(url, **kwargs) as response:
            response.raise_for_status()
            text = None

            if get_text:
                text = await response.text()

            content = await response.read()

            return ResponseWrapper(text, content)

    async def request_post(self, url: str, get_text: bool = True, **kwargs):
        await self._ensure_session()
        async with self.session.post(url, **kwargs) as response:
            response.raise_for_status()
            text = None

            if get_text:
                text = await response.text()

            content = await response.read()

            return ResponseWrapper(text, content)
