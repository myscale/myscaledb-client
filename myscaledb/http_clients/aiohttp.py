from typing import Any, AsyncGenerator, List, Optional

from aiohttp import ClientSession

from myscaledb.common.exceptions import ClientError
from myscaledb.http_clients.abc import HttpClientABC


class AiohttpHttpClient(HttpClientABC):
    line_separator: bytes = b'\n'

    def __init__(self, session: Optional[ClientSession]):
        if session:
            self._session = session
        else:
            self._session = ClientSession()

    async def get(self, url: str, params: dict) -> None:
        async with self._session.get(url=url, params=params) as resp:
            await _check_response(resp)

    async def post_return_lines(
            self, url: str, params: dict, data: Any
    ) -> AsyncGenerator[bytes, None]:
        resp = None
        # to show the exception stack
        post_success = False
        for i in range(0, 5):
            try:
                resp = await self._session.post(url=url, params=params, data=data)
                post_success = True
                break
            except Exception as e:
                continue
        if not post_success:
            resp = await self._session.post(url=url, params=params, data=data)
        # check resp status
        await _check_response(resp)
        buffer: bytes = b''
        async for chunk in resp.content.iter_any():
            lines: List[bytes] = chunk.split(self.line_separator)
            lines[0] = buffer + lines[0]
            buffer = lines.pop(-1)
            for line in lines:
                yield line + self.line_separator
        assert not buffer
        resp.close()

    async def post_no_return(self, url: str, params: dict, data: Any) -> None:
        # (f"trying to upload: {params}--{data}")
        resp = None
        # to show the exception stack
        post_success = False
        for i in range(0, 5):
            try:
                resp = await self._session.post(url=url, params=params, data=data)
                post_success = True
                break
            except Exception as e:
                continue
        if not post_success:
            resp = await self._session.post(url=url, params=params, data=data)
        # check resp status
        await _check_response(resp)
        resp.close()

    async def close(self) -> None:
        await self._session.close()


async def _check_response(resp):
    if resp.status != 200:
        raise ClientError(await _read_error_body(resp))


async def _read_error_body(resp):
    return (await resp.read()).decode(errors='replace')
