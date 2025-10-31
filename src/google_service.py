import asyncio
from typing import Dict, List, Optional, Tuple
import aiohttp
from models.book import Book
from utils.config import Config
from utils.logger import log_info

class GoogleService:
    def __init__(self, config: Config):
        self.api_key = config.api_key
        self.tags = config.get_tags()
        self.results_per_page = config.results_per_page
        self.max_pages = config.max_pages_per_tag
        self.semaphore = asyncio.Semaphore(config.max_concurrent_requests)
        connector = aiohttp.TCPConnector(
            limit=config.max_concurrent_requests,
            limit_per_host=config.max_concurrent_requests,
            enable_cleanup_closed=True,
        )
        timeout_seconds = getattr(config, "request_timeout", 30)
        self.timeout = aiohttp.ClientTimeout(total=timeout_seconds)
        self._connector = connector
        self._session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self):
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(connector=self._connector, timeout=self.timeout)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
            log_info("aiohttp session closed")

    async def fetch_books(self, books_map: Dict[str, Book]) -> Dict[str, Book]:
        """
        Fetch multiple endpoints concurrently (limited by semaphore and connector limits).
        Updates the provided books_map in-place and returns it.
        """
        endpoints = self._generate_endpoints()
        tasks = [asyncio.create_task(self._fetch_endpoint(url, books_map)) for url in endpoints]
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for r in results:
                if isinstance(r, Exception):
                    log_info(f"fetch_books task exception: {r}")
        return books_map

    async def _fetch_endpoint(self, url: str, books_map: Dict[str, Book]):
        async with self.semaphore:
            if self._session is None:
                self._session = aiohttp.ClientSession(connector=self._connector, timeout=self.timeout)
            try:
                async with self._session.get(url) as resp:
                    log_info(f"fetched: {resp.status} -> {url}")
                    if resp.status != 200:
                        return
                    data = await resp.json()
                    items = data.get("items") or []
                    for item in items:
                        _id = item.get("id")
                        if _id and _id not in books_map:
                            books_map[_id] = Book.from_api(_id, item)
            except Exception as e:
                log_info(f"_fetch_endpoint error for {url}: {e}")

    async def fetch_book_thumbnails(self, books_map: Dict[str, Book]) -> Dict[str, Book]:
        """
        Avoid deepcopy for performance: build a new dict with only books that have thumbnails.
        """
        if not books_map:
            return {}

        tasks: List[asyncio.Task] = []
        for _id, book in books_map.items():
            if book.self_link:
                tasks.append(asyncio.create_task(self._fetch_thumbnail(_id, book.self_link)))

        results = []
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)

        books_with_thumbs: Dict[str, Book] = {}
        for res in results:
            if isinstance(res, Exception):
                log_info(f"_fetch_thumbnail task exception: {res}")
                continue
            if res is None:
                continue
            _id, thumb = res
            if thumb:
                book = books_map.get(_id)
                if book:
                    book.thumbnail_url = thumb
                    books_with_thumbs[_id] = book

        return books_with_thumbs

    async def _fetch_thumbnail(self, _id: str, self_link: str) -> Optional[Tuple[str, Optional[str]]]:
        async with self.semaphore:
            if self._session is None:
                self._session = aiohttp.ClientSession(connector=self._connector, timeout=self.timeout)
            try:
                async with self._session.get(self_link) as resp:  # type: ignore
                    if resp.status != 200:
                        return (_id, None)
                    data = await resp.json()
                    links = data.get("volumeInfo", {}).get("imageLinks", {})
                    thumb = links.get("medium") or links.get("thumbnail")
                    return (_id, thumb)
            except Exception as e:
                log_info(f"Error fetching thumbnail for {_id}: {e}")
                return (_id, None)

    def _generate_endpoints(self) -> list[str]:
        endpoints = []
        for tag in self.tags:
            for start in range(0, self.max_pages * 40, 40):
                url = (
                    f"https://www.googleapis.com/books/v1/volumes?"
                    f"q={tag}&startIndex={start}&maxResults={self.results_per_page}&key={self.api_key}&langRestrict=pt"
                )
                endpoints.append(url)
        return endpoints
