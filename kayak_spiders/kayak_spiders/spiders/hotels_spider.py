# ¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨#
#           LIBRARIES          #
# ______________________________#

import os
import pandas as pd
from io import StringIO
import scrapy
import datetime
import re

# ¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨#
#         AWS Settings         #
# ______________________________#

import boto3
s3 = boto3.resource('s3')
client = boto3.client('s3')
bucket_name = "kayak-jedha-certification-2023"
object_key = "top-5_destinations.csv"

session = boto3.Session(
    aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY')
)

# ¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨#
#             MAIN              #
# ______________________________#

# import cities_infos.csv from s3 bucket
csv_obj = client.get_object(Bucket=bucket_name, Key=object_key)
csv_string = csv_obj['Body'].read().decode('utf-8')
print(f'{object_key} has been import\n\n')

# create dataframe of cities
df = pd.read_csv(StringIO(csv_string))
cities = list(df['name'])
filename = "hotel_list.csv"

class HotelSpider(scrapy.Spider):
    name = 'booking_hotels_spider'
    allowed_domains = ['booking.com']
    site_url = "https://www.booking.com/index.fr-fr.html"
    checkin = str(datetime.date.today())
    checkout = str(datetime.date.today() + datetime.timedelta(days=7))
    start_urls = []

    # AWS IDS are already set in settings.py
    custom_settings = {
        'FEED_EXPORT_ENCODING' : 'utf-8',
        'FEEDS': {
            f"s3://kayak-jedha-certification-2023/{name}/{filename}": {
                "format": "csv",
            }
        }
    }

    for city in cities:
        start_urls.append('https://www.booking.com/searchresults.fr.html?checkin='+checkin +
                          '&ss='+city+'&no_rooms=1&checkout='+checkout+'&lang=fr&order=review_score_and_price&nflt=distance%3D10000')

    def parse(self, response):

        origin_url = response.request.url

        for hotel in response.css('div[data-testid="property-card"]'):
            hotel_url = hotel.css('a[data-testid="title-link"]::attr(href)').get()
            # I want to keep the information about the original city used for the request because,
            # all hotels does not concern the requested city in case of hotel non-availability
            city = re.search(r'&ss=(.*?)&', origin_url).group(1).replace('%20', ' ') 
            yield response.follow(hotel_url, callback=self.hotel_detail, cb_kwargs={"city": city})

    def hotel_detail(self, response, city):
        yield {
            'city': city,
            'hotel_url': response.url,
            'hotel_name': response.xpath('//*[@id="hp_hotel_name"]/div/div/h2/text()').get(),
            "hotel_description": response.xpath('//*[@id="property_description_content"]/p/text()').getall(),
            'hotel_score':  response.xpath('//*[@id="js--hp-gallery-scorecard"]/a/div/div/div/div/div[1]/text()').get(),
            'hotel_coord': response.xpath('//*[@id="showMap2"]/a/@data-atlas-latlng').get(),
            'hotel_adress': response.xpath('//*[@id="showMap2"]/span[1]/text()').get().replace('\n', '')
        }
