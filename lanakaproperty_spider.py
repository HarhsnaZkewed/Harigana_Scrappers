import scrapy
from scrapy.crawler import CrawlerProcess
import pymongo

class LankaPropertySpider(scrapy.Spider):
    name = 'lankaproperty'
    page_number = 1
    base_url = 'https://www.lankapropertyweb.com/sale/index.php?page={}&no-rooms=&search=1&radius='
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
        self.client = pymongo.MongoClient('mongodb+srv://harshanabuddhika9:uh4Av1QRBqmhXjwL@cluster0.bgvrx7w.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0')
        self.db = self.client['property']  # Database name
        self.collection = self.db['lanakaproperty_tb']  # Collection name

    def parse(self, response):
        """Extracts listing links and follows them to property details page."""
        listings = response.xpath("//h4[@class='listing-title']/a/@href").getall()

        for listing in listings:
            yield response.follow(response.urljoin(listing), callback=self.parse_property_details)

        # Pagination logic (up to page 5)
        if listings and self.page_number < 5:
            self.page_number += 1
            yield scrapy.Request(self.base_url.format(self.page_number), callback=self.parse)

    def parse_property_details(self, response):
        """Extracts property details from individual listing pages."""
        property_data = {
            'url': response.url,
            'title': response.xpath("normalize-space(//h1/text())").get(),
            'location': response.xpath("normalize-space(//div[@class='location title-light-1'])").get(),
            'bedrooms': response.xpath("//div[contains(text(),'Bedrooms')]/following-sibling::div/text()").get(),
            'bathrooms': response.xpath("//div[contains(text(),'Bathrooms/WCs')]/following-sibling::div/text()").get(),
            'floor_area': response.xpath("//div[contains(text(),'Floor area')]/following-sibling::div/text()").get(),
            'land_area': response.xpath("//div[contains(text(),'Area of land')]/following-sibling::div/text()").get(),
            'price': response.xpath("normalize-space(//span[@class='main_price mb-3 mb-sm-0']/text())").get(),
            'property_details': response.xpath("//div[@id='Property_Details']//p/text()").getall(),
            'features': response.xpath("//div[@id='Property_Features']//div[@class='item']/text()").getall(),
            'property_type': response.xpath("//div[contains(text(),'Property Type')]/following-sibling::div/text()").get(),
        }
        
         # Insert data into MongoDB
        self.collection.insert_one(property_data)
        self.log(f"Inserted property: {property_data['title']}")
        
    def closed(self, reason):
        """Close MongoDB connection when spider finishes."""
        self.client.close()


if __name__ == '__main__':
    process = CrawlerProcess()
    process.crawl(LankaPropertySpider)
    process.start()
