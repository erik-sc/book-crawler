import json
import os
from dotenv import load_dotenv

class Config:
    
    def __init__(self, path_to_config):
        load_dotenv()
        with open(path_to_config, "r", encoding="utf-8") as f:
            config = json.load(f)
            self.api_key = os.getenv("GOOGLE_API_KEY")
            self.max_concurrent_requests = config.get("max_concurrent_requests", 5)
            self.max_pages_per_tag = config.get("max_pages_per_tag", 5)
            self.data_dir = config.get("data_dir")
            self.images_dir = config.get("images_dir")
            self.output_csv = config.get("output_csv")
            self.tags_file = config.get("tags_file")
            self.results_per_page = config.get("results_per_page")
            self.make_folders()
            
    def make_folders(self):
        os.makedirs(self.images_dir, exist_ok=True)
        os.makedirs(self.data_dir, exist_ok=True)

    def get_tags(self):
        with open(self.tags_file, "r", encoding="utf-8") as f:
            return [line.strip() for line in f if line.strip()]
