import asyncio
import re
import json
from playwright.async_api import async_playwright  # Asynchronous API for browser automation
from DetailsScraper import DetailsScraping         # Custom module to extract animal details per brand

class AnimalScraper:
    def __init__(self, url):
        self.url = url  # Base URL to start scraping from (e.g. a category page)
        self.data = []  # List to store extracted brand and animal details

    async def scrape_brands_and_types(self):
        # Launch a Playwright browser context asynchronously
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)  # Start headless Chromium browser
            page = await browser.new_page()                   # Open a new browser tab
            await page.goto(self.url)                         # Navigate to the main URL

            # Select all brand links from the page using a CSS class selector
            brand_elements = await page.query_selector_all('.styles_itemWrapper__MTzPB a')

            # Check if any brand elements were found; if not, log and return empty data
            if not brand_elements:
                print(f"No brand elements found on {self.url}")
                return self.data

            # Loop through each brand link element found
            for element in brand_elements:
                title = await element.get_attribute('title')     # Extract the title (brand name)
                brand_link = await element.get_attribute('href') # Extract the relative or absolute URL

                if brand_link:
                    # Construct full URL if the href is relative
                    base_url = self.url.split('/', 3)[0] + '//' + self.url.split('/', 3)[2]
                    full_brand_link = base_url + brand_link if brand_link.startswith('/') else brand_link

                    # Log the full brand link being scraped
                    print(f"Full brand link: {full_brand_link}")

                    # Open a new tab to visit the brand's specific page
                    new_page = await browser.new_page()
                    await new_page.goto(full_brand_link)

                    # Use custom scraper to get animal details from this brand page
                    details_scraper = DetailsScraping(full_brand_link)
                    animal_details = await details_scraper.get_animal_details()

                    await new_page.close()  # Close the tab once done

                    # Store the results with brand info and list of animals
                    self.data.append({
                        'brand_title': title,  # Brand name
                        'brand_link': full_brand_link.rsplit('/', 1)[0] + '/{}',  # URL template for pagination
                        'available_animals': animal_details,  # List of extracted animals
                    })

                    # Log the extracted brand info
                    print(f"Found brand: {title}, Link: {full_brand_link}")

            await browser.close()  # Close the browser once all brands are processed

        return self.data  # Return the compiled data list
