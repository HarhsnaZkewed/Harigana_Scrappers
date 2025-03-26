import scrapy
from scrapy.crawler import CrawlerProcess
import pymongo
from datetime import datetime

class IkmanSpider(scrapy.Spider):
    name = 'ikman'
    page_number = 5
    base_url = 'https://ikman.lk/en/ads/sri-lanka/property?sort=date&order=desc&buy_now=0&urgent=0&page={}'
    start_urls = [base_url.format(page_number)]
    
    # Custom Scrapy settings for handling delays and retries
    custom_settings = {
        'DOWNLOAD_DELAY': 2,  # Wait 2 seconds between requests
        'RANDOMIZE_DOWNLOAD_DELAY': True,  # Add randomness to avoid detection
        'CONCURRENT_REQUESTS_PER_DOMAIN': 2,  # Reduce concurrent requests
        'RETRY_TIMES': 5,  # Retry failed requests up to 5 times
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    }

    def __init__(self):
        """Initialize MongoDB connection."""
        self.client = pymongo.MongoClient("mongodb+srv://zkewed:zkewed123A@vehicalevaluation.d9ufa.mongodb.net/?retryWrites=true&w=majority", 27017)
        self.db = self.client['data_store_dev']  # Database name
        self.collection = self.db['ikman_land_tb']  # Collection name
    
    def parse(self, response):
        """Extracts listing links and follows them to property details page."""
        listings = response.xpath('//li[@class="normal--2QYVk gtm-normal-ad"]/a/@href').getall()

        for listing in listings:
            yield response.follow(response.urljoin(listing), callback=self.parse_property_details)

        # Pagination logic (up to page 5)
        if listings and self.page_number < 2:
            self.page_number += 1
            yield scrapy.Request(self.base_url.format(self.page_number), callback=self.parse)

    def parse_property_details(self, response):
        """Extracts property details from individual listing pages."""
        property_data = {
            'url': response.url,
            'title': response.xpath("normalize-space(//h1[@class='title--3s1R8']/text())").get(),
            'location': response.xpath("//div[contains(@class, 'word-break--2nyVq') and contains(text(),'Address:')]/following-sibling::div/text()").get(),
            'bedrooms': response.xpath("//div[contains(@class, 'word-break--2nyVq') and contains(text(),'Bedrooms:')]/following-sibling::div//span/text()").get(),
            'bathrooms': response.xpath("//div[contains(@class, 'word-break--2nyVq') and contains(text(),'Bathrooms:')]/following-sibling::div/text()").get(),
            'floor_area': response.xpath("//div[contains(@class, 'word-break--2nyVq') and contains(text(),'House size:')]/following-sibling::div/text()").get(),
            'land_area': response.xpath("//div[contains(@class, 'word-break--2nyVq') and contains(text(),'Land size:')]/following-sibling::div/text()").get(),
            'price': response.xpath("normalize-space(//div[@class='amount--3NTpl']/text())").get(),
            'property_details': response.xpath("//div[@class='description--1nRbz']//p/text()")[0].get().strip(),
            'features': response.xpath("//div[@class='description--1nRbz']//p/text()").getall(),
            'property_type': response.xpath("normalize-space(//h1[@class='title--3s1R8']/text())").get(),
            'inserted_datetime':datetime.now(),
        }
        
        # Insert data into MongoDB
        self.collection.insert_one(property_data)
        self.log(f"Inserted property: {property_data['title']}")

    def closed(self, reason):
        """Close MongoDB connection when spider finishes."""
        self.client.close()


if __name__ == '__main__':
    process = CrawlerProcess()
    process.crawl(IkmanSpider)
    process.start()
