import aiohttp
import asyncio
import csv
from data_service import DataService
from google_service import GoogleService
from utils.config import Config
from utils.logger import log_info

async def main():
    log_info(f"Iniciando crawler...")

    config = Config('config.json')
    google = GoogleService(config)
    data = DataService(config)

    tags = config.get_tags()

    book_map = data.get_current_map()
    
    log_info(book_map)

    books = await google.fetch_books(book_map)

    log_info(f"Livros obtidos {books}")

    images = await google.fetch_book_thumbnails(books)

    log_info(f"Iniciando save...")

    await data.save_data(books, images)

if __name__ == "__main__":
    asyncio.run(main())
