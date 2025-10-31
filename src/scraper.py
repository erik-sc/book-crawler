import aiohttp
import asyncio
import csv
from data_service import DataService
from google_service import GoogleService
from utils.config import Config
from utils.logger import log_info

async def main():
    config = Config('config.json')
    google = GoogleService(config)
    data = DataService(config)

    book_map = data.get_current_map()

    log_info(f"Loaded {len(book_map)} existing books from CSV")

    books = await google.fetch_books(book_map)

    log_info(f"Fetched {len(books)} books from Google API")

    books_with_thumbnail = await google.fetch_book_thumbnails(books)

    log_info(f"Fetched {len(books_with_thumbnail)} book thumbnails from Google API")

    await data.save_data(books_with_thumbnail)

    await data.close()
    await google.close()

if __name__ == "__main__":
    asyncio.run(main())
