import aiofiles
import csv
import os
import aiohttp
from pathlib import Path

from utils.logger import log_info

class DataService:
    def __init__(self, config):
        self.data_dir = Path(config.data_dir)
        self.images_dir = self.data_dir / "images"
        self.csv_file = self.data_dir / "books.csv"
        self.images_dir.mkdir(parents=True, exist_ok=True)

    async def save_data(self, books_map, images):
        async with aiohttp.ClientSession() as session:
            for id, book in books_map.items():
                img_url = images.get(id)
                await self._save_image(img_url, id, session)
                await self._save_csv(book, id, img_url)
    
    async def _save_csv(self, book, id, img_url):
        async with aiofiles.open(self.csv_file, "a", encoding="utf-8") as f:
            await f.write(
                f"{id},{book['title']},{'|'.join(book['authors'])},{'|'.join(book['categories'])},{book['selfLink']},{img_url}\n"
            )

    async def _save_image(self, img_url, id, session):
        img_path = None
        img_path = self.images_dir / f"{id}.jpg"
        try:
            async with session.get(img_url) as resp:
                if resp.status == 200:
                    content = await resp.read()
                    async with aiofiles.open(img_path, "wb") as f:
                        await f.write(content)
        except Exception as e:
            log_info(f"Erro ao baixar imagem {id}: {e}")

    def get_current_map(self):
        books_map = {}
        if not self.csv_file.exists():
            return books_map

        with open(self.csv_file, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            for row in reader:
                if len(row) < 2:
                    continue
                vid = row[0]
                title = row[1]
                books_map[vid] = {"title": title}
        return books_map
