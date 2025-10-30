import asyncio
import aiohttp
from utils.config import Config
from utils.logger import log_info

class GoogleService:
    def __init__(self, config: Config):
        self.api_key = config.api_key
        self.tags = config.get_tags()
        self.max_pages = config.max_pages_per_tag
        self.session = None
        self.semaphore = asyncio.Semaphore(config.max_concurrent_requests)

    async def fetch_books(self, books_map):
        async with aiohttp.ClientSession() as session:
            for tag in self.tags:
                log_info(f"Buscando {tag}")
                for start in range(0, self.max_pages * 40, 40):
                    url = (
                        f"https://www.googleapis.com/books/v1/volumes?"
                        f"q={tag}&startIndex={start}&maxResults=40&key={self.api_key}&langRestrict=pt"
                    )
                    async with self.semaphore:
                        async with session.get(url) as resp:
                            data = await resp.json()
                            log_info(f"Dados encontrados: {data}")
                            for item in data.get("items", []):
                                vid = item.get("id")
                                if vid not in books_map:
                                    if(item.get("selfLink") is None):
                                        log_info(f"Livro não possuia selfLink! - {item.get("id")}")
                                        continue
                                    books_map[vid] = {
                                        "title": item["volumeInfo"].get("title"),
                                        "authors": item["volumeInfo"].get("authors", []),
                                        "categories": item["volumeInfo"].get("categories", []),
                                        "selfLink": item.get("selfLink"),
                                    }
        return books_map

    async def fetch_book_thumbnails(self, books_map):
        images = {}
        no_thumb = set()

        async with aiohttp.ClientSession() as session:
            tasks = []
            for vid, book in books_map.items():
                tasks.append(self._fetch_thumbnail(session, vid, book["selfLink"], images, no_thumb))
            await asyncio.gather(*tasks)

        # Remove livros sem thumbnail do mapa
        for vid in no_thumb:
            books_map.pop(vid, None)

        return images


    async def _fetch_thumbnail(self, session, vid, self_link, images, no_thumb):
        async with self.semaphore:
            try:
                async with session.get(self_link) as resp:
                    if resp.status != 200:
                        no_thumb.add(vid)
                        return

                    data = await resp.json()
                    links = data.get("volumeInfo", {}).get("imageLinks", {})
                    thumb = links.get("medium") or links.get("thumbnail")

                    if thumb:
                        images[vid] = thumb
                    else:
                        no_thumb.add(vid)
            except Exception as e:
                print(f"Erro ao buscar thumbnail: {e}")
                no_thumb.add(vid)

