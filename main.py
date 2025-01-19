import asyncio
import pandas as pd
import os
import json
import logging
from playwright.async_api import async_playwright
from datetime import datetime, timedelta
from DetailsScraper import DetailsScraping
from SavingOnDrive import SavingOnDrive
from typing import Dict, List, Tuple
from pathlib import Path

class NormalMainScraper:
    def __init__(self, automotives_data: Dict[str, List[Tuple[str, int]]]):
        self.automotives_data = automotives_data
        self.chunk_size = 2  # Number of automotives processed per chunk
        self.max_concurrent_links = 2  # Max links processed simultaneously
        self.logger = logging.getLogger(__name__)
        self.setup_logging()
        self.temp_dir = Path("temp_files")
        self.temp_dir.mkdir(exist_ok=True)
        self.upload_retries = 3
        self.upload_retry_delay = 15  # Retry delay in seconds
        self.page_delay = 3  # Delay between page requests
        self.chunk_delay = 10  # Delay between chunks

    def setup_logging(self):
        """Initialize logging configuration."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler('scraper.log')
            ]
        )
        self.logger.setLevel(logging.INFO)

    async def scrape_automotive(self, automotive_name: str, urls: List[Tuple[str, int]], semaphore: asyncio.Semaphore) -> List[Dict]:
        """Scrape data for a single automotive category."""
        self.logger.info(f"Starting to scrape {automotive_name}")
        animal_data = []
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        async with semaphore:
            for url_template, page_count in urls:
                for page in range(1, page_count + 1):
                    url = url_template.format(page)
                    scraper = DetailsScraping(url)
                    try:
                        animals = await scraper.get_animal_details()
                        for animal in animals:
                            if animal.get("date_published", "").split()[0] == yesterday:
                                animal_data.append(animal)
                        await asyncio.sleep(self.page_delay)  # Delay between page requests
                    except Exception as e:
                        self.logger.error(f"Error scraping {url}: {e}")
                        continue  # Continue with next page even if current fails

        return animal_data

    async def save_to_excel(self, automotive_name: str, animal_data: List[Dict]) -> str:
        """Save data to an Excel file."""
        if not animal_data:
            self.logger.info(f"No data to save for {automotive_name}, skipping Excel file creation.")
            return None

        excel_file = Path(f"{automotive_name}.xlsx")
        
        try:
            df = pd.DataFrame(animal_data)
            df.to_excel(excel_file, index=False)
            self.logger.info(f"Successfully saved data for {automotive_name}")
            return str(excel_file)
        except Exception as e:
            self.logger.error(f"Error saving Excel file {excel_file}: {e}")
            return None

    async def upload_files_with_retry(self, drive_saver, files: List[str]) -> List[str]:
        """Upload files to Google Drive with retry mechanism."""
        uploaded_files = []
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        try:
            # Get or create yesterday's folder directly in parent folder
            folder_id = drive_saver.get_folder_id(yesterday)
            if not folder_id:
                folder_id = drive_saver.create_folder(yesterday)
                self.logger.info(f"Created new folder '{yesterday}'")

            for file in files:
                for attempt in range(self.upload_retries):
                    try:
                        if os.path.exists(file):
                            drive_saver.save_files([file], folder_id=folder_id)
                            uploaded_files.append(file)
                            self.logger.info(f"Successfully uploaded {file} to Google Drive folder '{yesterday}'")
                            break
                    except Exception as e:
                        self.logger.error(f"Upload attempt {attempt + 1} failed for {file}: {e}")
                        if attempt < self.upload_retries - 1:
                            await asyncio.sleep(self.upload_retry_delay)
                            try:
                                drive_saver.authenticate()
                            except Exception as auth_error:
                                self.logger.error(f"Failed to refresh authentication: {auth_error}")
                        else:
                            self.logger.error(f"Failed to upload {file} after {self.upload_retries} attempts")

        except Exception as e:
            self.logger.error(f"Error managing Google Drive folder for {yesterday}: {e}")

        return uploaded_files

    async def scrape_all_automotives(self):
        """Scrape all automotives in chunks."""
        self.temp_dir.mkdir(exist_ok=True)

        # Setup Google Drive
        try:
            credentials_json = os.environ.get('ANIMALS_GCLOUD_KEY_JSON')
            if not credentials_json:
                raise EnvironmentError("ANIMALS_GCLOUD_KEY_JSON environment variable not found")
            credentials_dict = json.loads(credentials_json)
            drive_saver = SavingOnDrive(credentials_dict)
            drive_saver.authenticate()
        except Exception as e:
            self.logger.error(f"Failed to setup Google Drive: {e}")
            return

        # Split automotives into chunks
        automotive_chunks = [
            list(self.automotives_data.items())[i:i + self.chunk_size]
            for i in range(0, len(self.automotives_data), self.chunk_size)
        ]

        semaphore = asyncio.Semaphore(self.max_concurrent_links)

        for chunk_index, chunk in enumerate(automotive_chunks, 1):
            self.logger.info(f"Processing chunk {chunk_index}/{len(automotive_chunks)}")

            tasks = []
            for automotive_name, urls in chunk:
                task = asyncio.create_task(self.scrape_automotive(automotive_name, urls, semaphore))
                tasks.append((automotive_name, task))
                await asyncio.sleep(2)  # Delay between task creation

            pending_uploads = []
            for automotive_name, task in tasks:
                try:
                    animal_data = await task
                    if animal_data:
                        excel_file = await self.save_to_excel(automotive_name, animal_data)
                        if excel_file:
                            pending_uploads.append(excel_file)
                except Exception as e:
                    self.logger.error(f"Error processing {automotive_name}: {e}")

            if pending_uploads:
                uploaded_files = await self.upload_files_with_retry(drive_saver, pending_uploads)

                # Clean up uploaded files
                for file in pending_uploads:
                    try:
                        os.remove(file)
                        self.logger.info(f"Cleaned up local file: {file}")
                    except Exception as e:
                        self.logger.error(f"Error cleaning up {file}: {e}")

            if chunk_index < len(automotive_chunks):
                self.logger.info(f"Waiting {self.chunk_delay} seconds before next chunk...")
                await asyncio.sleep(self.chunk_delay)

if __name__ == "__main__":
    automotives_data = {
        "كلاب": [("https://www.q84sale.com/ar/animals/dogs/{}", 1)],
        "قطط": [("https://www.q84sale.com/ar/animals/cats/{}", 2)],
        "طيور": [("https://www.q84sale.com/ar/animals/birds/{}", 7)],
        "الخيل": [("https://www.q84sale.com/ar/animals/horses/{}", 2)],
        "الماشية": [("https://www.q84sale.com/ar/animals/sheep/{}", 3)],
        "الابل": [("https://www.q84sale.com/ar/animals/camels/{}", 2)],
        "معدات الحيوانات و الطيور": [("https://www.q84sale.com/ar/animals/animal-and-pet-equipment/{}", 1)],
        "أعلاف": [("https://www.q84sale.com/ar/animals/animal-and-pet-food/{}", 1)],
        "خدمات الكلاب": [("https://www.q84sale.com/ar/animals/dogs-services/{}", 1)],
        "خدمات الخيل": [("https://www.q84sale.com/ar/animals/horse-services/{}", 1)],
        "خدمات الماشية": [("https://www.q84sale.com/ar/animals/sheep-services/{}", 1)],
        "حيوانات اخرى": [("https://www.q84sale.com/ar/animals/other-animals-and-pets/{}", 1)],
    }

    async def main():
        scraper = NormalMainScraper(automotives_data)
        await scraper.scrape_all_automotives()

    # Run everything in the async event loop
    asyncio.run(main())
