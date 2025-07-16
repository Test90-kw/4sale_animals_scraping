import pandas as pd
import json
import asyncio
import nest_asyncio
import re
from playwright.async_api import async_playwright
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

# Allow nested event loops (important for running async code in environments like Jupyter)
nest_asyncio.apply()

class DetailsScraping:
    def __init__(self, url, retries=3):
        self.url = url                # URL of the animal listing page
        self.retries = retries        # Number of retries in case of failure

    # Main method to extract details from a listing page
    async def get_animal_details(self):
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)  # Launch headless browser
            page = await browser.new_page()                   # Open a new tab

            # Set default timeouts
            page.set_default_navigation_timeout(30000)
            page.set_default_timeout(30000)

            animals = []  # List to hold all extracted animals

            # Retry mechanism for robustness
            for attempt in range(self.retries):
                try:
                    await page.goto(self.url, wait_until="domcontentloaded")  # Load the page
                    await page.wait_for_selector('.StackedCard_card__Kvggc', timeout=30000)  # Wait for cards

                    # Select all animal cards on the page
                    animal_cards = await page.query_selector_all('.StackedCard_card__Kvggc')
                    for card in animal_cards:
                        # Extract details from each card
                        link = await self.scrape_link(card)
                        animal_type = await self.scrape_animal_type(card)
                        title = await self.scrape_title(card)
                        pinned_today = await self.scrape_pinned_today(card)

                        # Visit detail page for more information
                        scrape_more_details = await self.scrape_more_details(link)

                        # Combine all details into one dictionary
                        animals.append({
                            'id': scrape_more_details.get('id'),
                            'date_published': scrape_more_details.get('date_published'),
                            'relative_date': scrape_more_details.get('relative_date'),
                            'pin': pinned_today,
                            'type': animal_type,
                            'title': title,
                            'description': scrape_more_details.get('description'),
                            'link': link,
                            'image': scrape_more_details.get('image'),
                            'price': scrape_more_details.get('price'),
                            'address': scrape_more_details.get('address'),
                            'additional_details': scrape_more_details.get('additional_details'),
                            'specifications': scrape_more_details.get('specifications'),
                            'views_no': scrape_more_details.get('views_no'),
                            'submitter': scrape_more_details.get('submitter'),
                            'ads': scrape_more_details.get('ads'),
                            'membership': scrape_more_details.get('membership'),
                            'phone': scrape_more_details.get('phone'),
                        })
                    break  # Exit retry loop on success

                except Exception as e:
                    print(f"Attempt {attempt + 1} failed for {self.url}: {e}")
                    if attempt + 1 == self.retries:
                        print(f"Max retries reached for {self.url}. Returning partial results.")
                        break
                finally:
                    await page.close()  # Close the page after each attempt
                    if attempt + 1 < self.retries:
                        page = await browser.new_page()  # Reopen new page if retrying

            await browser.close()
            return animals

    # Extract href link from the card
    async def scrape_link(self, card):
        rawlink = await card.get_attribute('href')
        base_url = 'https://www.q84sale.com'
        return f"{base_url}{rawlink}" if rawlink else None

    # Extract the animal type/category
    async def scrape_animal_type(self, card):
        selector = '.text-6-med.text-neutral_600.styles_category__NQAci'
        element = await card.query_selector(selector)
        return await element.inner_text() if element else None

    # Extract title or name of the listing
    async def scrape_title(self, card):
        selector = '.text-4-med.text-neutral_900.styles_title__l5TTA.undefined'
        element = await card.query_selector(selector)
        return await element.inner_text() if element else None

    # Extract card's pin status
    async def scrape_pinned_today(self, card):
        selector = '.StackedCard_tags__SsKrH'
        element = await card.query_selector(selector)
        if element:
            content = await element.inner_html()
            if content.strip() != "":
                return "Pinned today"
        return "Not Pinned"

    # Extract relative posted time like "2 days ago"
    async def scrape_relative_date(self, page):
        try:
            parent_selector = '.d-flex.styles_topData__Sx1GF'
            parent_locator = page.locator(parent_selector)

            await parent_locator.wait_for(state="visible", timeout=10000)

            child_divs = parent_locator.locator('.d-flex.align-items-center.styles_dataWithIcon__For9u')
            await child_divs.first.wait_for(state="visible", timeout=10000)
            await child_divs.nth(1).wait_for(state="visible", timeout=10000)

            relative_time_locator = child_divs.nth(1).locator('div.text-5-regular.m-text-6-med.text-neutral_600')
            relative_time_text = await relative_time_locator.inner_text()
            return relative_time_text.replace(" ago", "").strip() if relative_time_text else None

        except Exception as e:
            print(f"Error while scraping relative_time value: {e}")
            return None

    # Convert relative date to actual publish date
    async def scrape_publish_date(self, relative_time):
        pattern = r'(\d+)\s+(Second|Minute|Hour|Day|Month|شهر|ثانية|دقيقة|ساعة|يوم)'
        match = re.search(pattern, relative_time, re.IGNORECASE)
        if not match:
            return "Invalid Relative Time"

        number = int(match.group(1))
        unit = match.group(2).lower()
        current_time = datetime.now()

        # Subtract time according to unit
        if unit in ["second", "ثانية"]:
            publish_time = current_time - timedelta(seconds=number)
        elif unit in ["minute", "دقيقة"]:
            publish_time = current_time - timedelta(minutes=number)
        elif unit in ["hour", "ساعة"]:
            publish_time = current_time - timedelta(hours=number)
        elif unit in ["day", "يوم"]:
            publish_time = current_time - timedelta(days=number)
        elif unit in ["month", "شهر"]:
            publish_time = current_time - relativedelta(months=number)
        else:
            return "Unsupported time unit found."

        return publish_time.strftime("%Y-%m-%d %H:%M:%S")

    # Extract number of views
    async def scrape_views_no(self, page):
        try:
            selector = '.d-flex.align-items-center.styles_dataWithIcon__For9u .text-5-regular.m-text-6-med.text-neutral_600'
            element = await page.query_selector(selector)
            return (await element.inner_text()).strip() if element else None
        except Exception as e:
            print(f"Error while scraping views number: {e}")
            return None

    # Extract ID from the listing
    async def scrape_id(self, page):
        parent_selector = '.el-lvl-1.d-flex.align-items-center.justify-content-between.styles_sectionWrapper__v97PG'
        parent_element = await page.query_selector(parent_selector)
        if not parent_element:
            return None

        ad_id_selector = '.text-4-regular.m-text-5-med.text-neutral_600'
        ad_id_element = await parent_element.query_selector(ad_id_selector)
        if not ad_id_element:
            return None

        text = await ad_id_element.inner_text()
        match = re.search(r'رقم الاعلان:\s*(\d+)', text)
        return match.group(1) if match else None

    # Extract image URL
    async def scrape_image(self, page):
        try:
            selector = '.styles_img__PC9G3'
            image = await page.query_selector(selector)
            return await image.get_attribute('src') if image else None
        except Exception as e:
            print(f"Error scraping image: {e}")
            return None

    # Extract price
    async def scrape_price(self, page):
        selector = '.h3.m-h5.text-prim_4sale_500'
        price = await page.query_selector(selector)
        return await price.inner_text() if price else "0 KWD"

    # Extract address
    async def scrape_address(self, page):
        selector = '.text-4-regular.m-text-5-med.text-neutral_600'
        address = await page.query_selector(selector)
        if address:
            text = await address.inner_text()
            return "Not Mentioned" if re.match(r'^رقم الاعلان: \d+$', text) else text
        return "Not Mentioned"

    # Extract features (like checkboxes or badges)
    async def scrape_additionalDetails_list(self, page):
        selector = '.styles_boolAttrs__Ce6YV .styles_boolAttr__Fkh_j div'
        elements = await page.query_selector_all(selector)

        values_list = []
        for element in elements:
            text = await element.inner_text()
            if text.strip():
                values_list.append(text.strip())

        return values_list

    # Extract structured specifications
    async def scrape_specifications(self, page):
        selector = '.styles_attrs__PX5Fs .styles_attr__BN3w_'
        elements = await page.query_selector_all(selector)

        attributes = {}
        for element in elements:
            img = await element.query_selector('img')
            if img:
                alt = await img.get_attribute('alt')
                text_el = await element.query_selector('.text-4-med.m-text-5-med.text-neutral_900')
                value = await text_el.inner_text() if text_el else None
                if alt and value:
                    attributes[alt] = value.strip()

        return attributes

    # Extract phone number from embedded JSON data
    async def scrape_phone_number(self, page):
        try:
            script_content = await page.inner_html('script#__NEXT_DATA__')
            data = json.loads(script_content.strip())
            return data.get("props", {}).get("pageProps", {}).get("listing", {}).get("phone", None)
        except Exception as e:
            print(f"Error while scraping phone number: {e}")
            return None

    # Extract submitter name, ads, and membership info
    async def scrape_submitter_details(self, page):
        wrapper_selector = '.styles_infoWrapper__v4P8_.undefined.align-items-center'
        wrappers = await page.query_selector_all(wrapper_selector)

        if wrappers:
            wrapper = wrappers[0]
            name_el = await wrapper.query_selector('.text-4-med.m-h6.text-neutral_900')
            submitter = await name_el.inner_text() if name_el else None

            detail_els = await wrapper.query_selector_all('.styles_memberDate__qdUsm span.text-neutral_600')
            ads, membership = "0 ads", "membership year not mentioned"

            for el in detail_els:
                txt = await el.inner_text()
                if re.match(r'^\d+\s+ads$', txt, re.IGNORECASE) or re.match(r'^\d+\s+اعلان$', txt):
                    ads = txt
                elif re.match(r'^عضو منذ \D+\s+\d+$', txt) or re.match(r'^member since \D+\s+\d+$', txt, re.IGNORECASE):
                    membership = txt

            return {
                'submitter': submitter,
                'ads': ads,
                'membership': membership
            }
        return {}

    # Aggregate and return all scraped details from a listing page
    async def scrape_more_details(self, url):
        retries = 3
        for attempt in range(retries):
            try:
                async with async_playwright() as p:
                    browser = await p.chromium.launch(headless=True)
                    page = await browser.new_page()
                    await page.goto(url, wait_until="domcontentloaded", timeout=60000)

                    id = await self.scrape_id(page)
                    description = await self.scrape_description(page)
                    image = await self.scrape_image(page)
                    price = await self.scrape_price(page)
                    address = await self.scrape_address(page)
                    additional_details = await self.scrape_additionalDetails_list(page)
                    specifications = await self.scrape_specifications(page)
                    views_no = await self.scrape_views_no(page)
                    submitter_details = await self.scrape_submitter_details(page)
                    phone = await self.scrape_phone_number(page)
                    relative_date = await self.scrape_relative_date(page)
                    date_published = await self.scrape_publish_date(relative_date) if relative_date else None

                    await browser.close()
                    return {
                        'id': id,
                        'description': description,
                        'image': image,
                        'price': price,
                        'address': address,
                        'additional_details': additional_details,
                        'specifications': specifications,
                        'views_no': views_no,
                        'submitter': submitter_details.get('submitter'),
                        'ads': submitter_details.get('ads'),
                        'membership': submitter_details.get('membership'),
                        'phone': phone,
                        'relative_date': relative_date,
                        'date_published': date_published,
                    }

            except Exception as e:
                print(f"Error while scraping more details from {url}: {e}")
                if attempt + 1 == retries:
                    print(f"Max retries reached for {url}. Returning partial results.")
                    return {}

        return {}
