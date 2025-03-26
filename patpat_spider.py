import scrapy
from scrapy.crawler import CrawlerProcess
import pymongo
from datetime import datetime


class PatPatSpider(scrapy.Spider):
    name = 'patpat'
    page_number = 5
    base_url = 'https://www.patpat.lk/property?page={}&city=&sub_category=&sub_category_name=&category=property&search_txt=&sort_by='
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
        self.collection = self.db['patpat_tb']  # Collection name


    def parse(self, response):
        """Extracts listing links and follows them to property details page."""
        listings = response.xpath('//div[@class="result-img col-lg-3 px-lg-0"]/a/@href').getall()

        for listing in listings:
            yield response.follow(response.urljoin(listing), callback=self.parse_property_details)

        # Pagination logic (up to page 5)
        if listings and self.page_number < 2:
            self.page_number += 1
            yield scrapy.Request(self.base_url.format(self.page_number), callback=self.parse)

    def parse_property_details(self, response):
        """Extracts property details from individual listing pages."""
        property_data =  {
            'url': response.url,
            'title': response.xpath("normalize-space(//h2[@class='item-title col-12 my-2']/text())").get(),
            'location': response.xpath('//th[normalize-space(text())="Location"]/following-sibling::td/text()').get(),
            'bedrooms':  response.xpath('//th[normalize-space(text())="Features"]/following-sibling::td/text()').re_first(r'Beds-(\d+)'),
            'bathrooms': response.xpath('//th[normalize-space(text())="Features"]/following-sibling::td/text()').re_first(r'Baths-(\d+)'),
            'floor_area': response.xpath('//th[normalize-space(text())="Area"]/following-sibling::td/text()').get(),
            'land_area': response.xpath('//th[normalize-space(text())="Land Size"]/following-sibling::td/text()').get(),
            'price': response.xpath('//div[@class="price-info"]//p[@class="price-value"]/text()').get(),
            'property_details': response.xpath('//th[normalize-space(text())="Types"]/following-sibling::td/text()').get(),
            'features':response.xpath('//div[@class="item-description card mt-3 mb-3 p-3"]//p/text()').getall(),
            'property_type': response.xpath('//th[normalize-space(text())="Category"]/following-sibling::td/text()').get(),
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
    process.crawl(PatPatSpider)
    process.start()
 