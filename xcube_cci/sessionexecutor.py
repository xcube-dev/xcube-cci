# The MIT License (MIT)
# Copyright (c) 2026 ESA Climate Change Initiative
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import asyncio
import random
import threading
from typing import Optional

import aiohttp

from .constants import (DEFAULT_NUM_RETRIES, DEFAULT_RETRY_BACKOFF_BASE,
                        DEFAULT_RETRY_BACKOFF_MAX, LOG)


class OpendapTimeoutError(Exception):
    pass


class SessionExecutor:

    def __init__(
            self,
            user_agent: str = None,
            enable_warnings: bool = False,
            num_retries: int = DEFAULT_NUM_RETRIES,
            retry_backoff_max: int = DEFAULT_RETRY_BACKOFF_MAX,
            retry_backoff_base: float = DEFAULT_RETRY_BACKOFF_BASE,
    ):
        self._headers = {'User-Agent': user_agent} if user_agent else None
        self._enable_warnings = enable_warnings
        self._num_retries = num_retries
        self._retry_backoff_max = retry_backoff_max
        self._retry_backoff_base = retry_backoff_base
        self._executor_loop = None
        self._executor_session = None
        self._executor_thread = None
        self._loop_lock = threading.Lock()

    def _ensure_executor_loop(self):
        if self._executor_loop is not None:
            return

        with self._loop_lock:
            if self._executor_loop is not None:
                return

            loop_created = threading.Event()

            def loop_runner():
                self._executor_loop = asyncio.new_event_loop()
                asyncio.set_event_loop(self._executor_loop)
                timeout = aiohttp.ClientTimeout(total=300, connect=30, sock_connect=30, sock_read=120)
                connector = aiohttp.TCPConnector(limit=50, loop=self._executor_loop, force_close=True)
                self._executor_session = aiohttp.ClientSession(
                    connector=connector,
                    headers=self._headers,
                    trust_env=True,
                    timeout=timeout
                )
                loop_created.set()
                self._executor_loop.run_forever()

            self._executor_thread = threading.Thread(
                target=loop_runner,
                daemon=True
            )
            self._executor_thread.start()

            loop_created.wait()

    def run_with_session(self, async_function, *params):
        self._ensure_executor_loop()

        async def _run_with_session_executor(e_function, *e_params):
            return await e_function(self._executor_session, *e_params)

        future = asyncio.run_coroutine_threadsafe(
            _run_with_session_executor(async_function, *params),
            self._executor_loop
        )
        return future.result()

    def get_response_content(self, url: str) -> Optional[bytes]:
        return self.run_with_session(self.get_response_content_from_session, url)

    async def get_response_content_from_session(
            self, session: aiohttp.ClientSession, url: str
    ) -> Optional[bytes]:
        num_retries = self._num_retries
        retry_backoff_max = self._retry_backoff_max
        retry_backoff_base = self._retry_backoff_base
        error_message = "Max number of retries exceeded"
        for i in range(num_retries):
            if i > 0:
                LOG.debug(f"Accessing {url} attempt #{i}")
            retry_min = 100
            try:
                async with session.get(url) as resp:
                    retry_min = int(resp.headers.get('Retry-After', '100'))
                    if resp.status == 200:
                        return await resp.read()
                    elif 500 <= resp.status < 600:
                        error_message = f"Error {resp.status}: Cannot access url '{url}'"
                        if self._enable_warnings:
                            LOG.warning(error_message)
                    elif resp.status == 429:
                        error_message = "Error 429: Too Many Requests."
                    elif resp.status == 404:
                        error_message = "Error 404: Dataset not found."
            except (
                    aiohttp.ClientConnectionError,
                    aiohttp.ServerDisconnectedError,
                    aiohttp.ClientPayloadError,
                    asyncio.TimeoutError,
            ) as e:
                error_message = str(e)
            retry_backoff = random.random() * retry_backoff_max
            retry_total = retry_min + retry_backoff
            if self._enable_warnings:
                LOG.info(
                    f"{error_message} - attempt {i + 1}/{num_retries}, "
                    f"retrying in {retry_total / 1000:.2f} s"
                )
            await asyncio.sleep(retry_total / 1000)
            retry_backoff_max *= retry_backoff_base
        raise OpendapTimeoutError(error_message)

    def __getstate__(self):
        # overwrite to allow for serialisation
        state = self.__dict__.copy()
        state["_executor_session"] = None
        state["_executor_loop"] = None
        state["_executor_thread"] = None
        state["_loop_lock"] = None
        return state


    def __setstate__(self, state):
        # overwrite to set clean state after de-serialisation
        self.__dict__.update(state)
        self._executor_session = None
        self._executor_loop = None
        self._executor_thread = None
        self._loop_lock = threading.Lock()
