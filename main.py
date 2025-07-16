import asyncio
import pandas as pd
import os
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from DetailsScraper import DetailsScraping  # Custom scraper for individual listing pages
from SavingOnDrive import SavingOnDrive     # Handles Google Drive operations
from typing import Dict, List, Tuple        # For type hinting

class NormalMainScraper:
    def __init__(self, automotives_data: Dict[str, List[Tuple[str, int]]]):
        self.automotives_data = automotives_data  # Category name mapped to list of (url_template, page_count)
        self.chunk_size = 2  # Number of categories to process in one batch
        self.max_concurrent_links = 2  # Max concurrent scraping per semaphore
        self.logger = logging.getLogger(__name__)
        self.setup_logging()  # Configure logging to console and file
        self.temp_dir = Path("temp_files")  # Local folder for temporary files
        self.temp_dir.mkdir(exist_ok=True)  # Create it if not exists
        self.upload_retries = 3  # Max upload attempts
        self.upload_retry_delay = 15  # Delay between upload retries
        self.page_delay = 3  # Delay between scraping each page
        self.chunk_delay = 10  # Delay between scraping each chunk
        self.yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")  # Used for filtering and folder naming

    def setup_logging(self):
        # Configure logger to write both to terminal and 'scraper.log' file
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler('scraper.log')
            ]
        )
        self.logger.setLevel(logging.INFO)

    # Scrapes one animal category (e.g., Dogs), using a semaphore to control concurrency
    async def scrape_automotive(self, automotive_name: str, urls: List[Tuple[str, int]], semaphore: asyncio.Semaphore) -> List[Dict]:
        self.logger.info(f"Starting to scrape {automotive_name}")
        animal_data = []

        async with semaphore:  # Limit concurrent access
            for url_template, page_count in urls:
                for page in range(1, page_count + 1):  # Loop over pages
                    url = url_template.format(page)  # Fill page number in URL
                    scraper = DetailsScraping(url)
                    try:
                        animals = await scraper.get_animal_details()  # Scrape listings
                        for animal in animals:
                            # Filter listings published yesterday only
                            if animal.get("date_published", "").split()[0] == self.yesterday:
                                animal_data.append(animal)
                        await asyncio.sleep(self.page_delay)  # Delay between pages
                    except Exception as e:
                        self.logger.error(f"Error scraping {url}: {e}")
                        continue

        return animal_data

    # Saves scraped data to an Excel file and returns the filename
    async def save_to_excel(self, automotive_name: str, animal_data: List[Dict]) -> str:
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

    # Uploads a list of files to Drive with retry logic
    async def upload_files_with_retry(self, drive_saver, files: List[str], folder_id: str) -> List[str]:
        uploaded_files = []

        for file in files:
            for attempt in range(self.upload_retries):  # Retry logic
                try:
                    if os.path.exists(file):
                        drive_saver.save_files([file], folder_id=folder_id)
                        uploaded_files.append(file)
                        self.logger.info(f"Successfully uploaded {file} to Google Drive folder '{self.yesterday}'")
                        break
                except Exception as e:
                    self.logger.error(f"Upload attempt {attempt + 1} failed for {file}: {e}")
                    if attempt < self.upload_retries - 1:
                        await asyncio.sleep(self.upload_retry_delay)
                        try:
                            drive_saver.authenticate()  # Re-authenticate if needed
                        except Exception as auth_error:
                            self.logger.error(f"Failed to refresh authentication: {auth_error}")
                    else:
                        self.logger.error(f"Failed to upload {file} after {self.upload_retries} attempts")

        return uploaded_files

    # Main orchestrator to scrape all animal categories, save Excel, and upload
    async def scrape_all_automotives(self):
        self.temp_dir.mkdir(exist_ok=True)

        try:
            # Load credentials from environment variable and setup Google Drive client
            credentials_json = os.environ.get('ANIMALS_GCLOUD_KEY_JSON')
            if not credentials_json:
                raise EnvironmentError("ANIMALS_GCLOUD_KEY_JSON environment variable not found")
            credentials_dict = json.loads(credentials_json)
            drive_saver = SavingOnDrive(credentials_dict)
            drive_saver.authenticate()

            # Use yesterday's date as folder name
            folder_id = drive_saver.get_folder_id(self.yesterday)
            if not folder_id:
                folder_id = drive_saver.create_folder(self.yesterday)
                self.logger.info(f"Created new folder '{self.yesterday}'")
        except Exception as e:
            self.logger.error(f"Failed to setup Google Drive: {e}")
            return

        # Break data into chunks to control memory/load
        automotive_chunks = [
            list(self.automotives_data.items())[i:i + self.chunk_size]
            for i in range(0, len(self.automotives_data), self.chunk_size)
        ]

        semaphore = asyncio.Semaphore(self.max_concurrent_links)

        for chunk_index, chunk in enumerate(automotive_chunks, 1):
            self.logger.info(f"Processing chunk {chunk_index}/{len(automotive_chunks)}")

            tasks = []
            for automotive_name, urls in chunk:
                # Start async scraping task for each category in this chunk
                task = asyncio.create_task(self.scrape_automotive(automotive_name, urls, semaphore))
                tasks.append((automotive_name, task))
                await asyncio.sleep(2)  # Stagger starting tasks slightly

            pending_uploads = []
            for automotive_name, task in tasks:
                try:
                    animal_data = await task  # Await the scraping result
                    if animal_data:
                        excel_file = await self.save_to_excel(automotive_name, animal_data)
                        if excel_file:
                            pending_uploads.append(excel_file)
                except Exception as e:
                    self.logger.error(f"Error processing {automotive_name}: {e}")

            if pending_uploads:
                await self.upload_files_with_retry(drive_saver, pending_uploads, folder_id)

                for file in pending_uploads:
                    try:
                        os.remove(file)
                        self.logger.info(f"Cleaned up local file: {file}")
                    except Exception as e:
                        self.logger.error(f"Error cleaning up {file}: {e}")

            # Wait before starting next chunk
            if chunk_index < len(automotive_chunks):
                self.logger.info(f"Waiting {self.chunk_delay} seconds before next chunk...")
                await asyncio.sleep(self.chunk_delay)

# Entry point of the script
if __name__ == "__main__":
    # Dictionary mapping category name to (URL template, number of pages)
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

    # Run everything in the async event loop
    async def main():
        scraper = NormalMainScraper(automotives_data)
        await scraper.scrape_all_automotives()

    asyncio.run(main())
