# Scrapy imports
import scrapy

# Name of the file where the results will be saved
filename = '35-cities-to-visit-in-france.json'

class TopcitiesfranceSpider(scrapy.Spider):
    name = 'top_french_cities_spider'
    start_urls = ['https://one-week-in.com/35-cities-to-visit-in-france/']

    # AWS IDS are already set in settings.py
    custom_settings = {
        'FEED_EXPORT_ENCODING' : 'utf-8',
        'FEEDS': {
            f"s3://kayak-jedha-certification-2023/{name}/{filename}": {
                "format": "json",
            }
        }
    }

    def parse(self, response):
        # we get the first <ol> element then text inside all <li> child
        cities = response.xpath("(//ol)[1]//li/a//text()").getall()
        yield {
            'cities': cities
        }

# go to kayak_spider/spiders folder and run the command below :
# Scrapy crawl top_french_cities_spider