from typing import Dict, List, Optional
import aiofiles
import csv
import os
import aiohttp
import asyncio
from pathlib import Path

from models.book import Book
from utils.logger import log_info


class DataService:

    def __init__(self, config):
        self.data_dir = Path(config.data_dir)
        self.images_dir = self.data_dir / "images"
        self.csv_file = self.data_dir / "books.csv"
        self.images_dir.mkdir(parents=True, exist_ok=True)

        max_concurrent = getattr(config, "max_concurrent_requests", 10)
        timeout_seconds = getattr(config, "request_timeout", 30)

        self.semaphore = asyncio.Semaphore(max_concurrent)
        self._connector = aiohttp.TCPConnector(limit=max_concurrent, limit_per_host=max_concurrent)
        self._timeout = aiohttp.ClientTimeout(total=timeout_seconds)
        self._session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self):
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(connector=self._connector, timeout=self._timeout)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
            log_info("DataService aiohttp session closed")

    async def save_data(self, books_with_thumbnail: Dict[str, Book]):
        """Download images concurrently (bounded) and append CSV rows for successful downloads.

        - Skips entries with no thumbnail URL
        - Writes CSV rows in a single async file write to avoid concurrent file I/O
        """
        if not books_with_thumbnail:
            return

        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(connector=self._connector, timeout=self._timeout)

        async def _download_and_flag(book_id: str, book: Book) -> Optional[Book]:
            img_url = book.thumbnail_url
            if not img_url:
                return None
            success = await self._save_image(img_url, book_id)
            return book if success else None

        tasks: List[asyncio.Task] = []
        for book_id, book in books_with_thumbnail.items():
            tasks.append(asyncio.create_task(_download_and_flag(book_id, book)))

        results = []
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)

        rows_to_write: List[str] = []
        for res in results:
            if isinstance(res, Exception):
                log_info(f"save_data task exception: {res}")
                continue
            if res is None:
                continue
            if isinstance(res, Book):
                rows_to_write.append(res.to_csv_row())

        if rows_to_write:
            async with aiofiles.open(self.csv_file, "a", encoding="utf-8") as f:
                await f.write("".join(rows_to_write))


    async def _save_image(self, img_url: str, id: str) -> bool:
        """Download single image using the shared session and semaphore. Returns True on success."""
        if not img_url:
            return False
        img_path = self.images_dir / f"{id}.jpg"

        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(connector=self._connector, timeout=self._timeout)

        try:
            async with self.semaphore:
                async with self._session.get(img_url) as resp:
                    if resp.status != 200:
                        log_info(f"Image download failed {id}: HTTP {resp.status}")
                        return False
                    content = await resp.read()
                    async with aiofiles.open(img_path, "wb") as f:
                        await f.write(content)
                    return True
        except Exception as e:
            log_info(f"Erro ao baixar imagem {id}: {e}")
            return False

    def get_current_map(self) -> Dict[str, Book]:
        books_map = {}
        if not self.csv_file.exists():
            return books_map

        with open(self.csv_file, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            for row in reader:
                if len(row) < 2:
                    continue
                vid = row[0]
                books_map[vid] = Book.from_csv(row)
        return books_map
