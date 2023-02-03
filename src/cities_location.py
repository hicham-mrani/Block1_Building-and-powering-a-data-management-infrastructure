#¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨#
#           LIBRARIES          #
#______________________________#

import os, requests, json
import pandas as pd
from io import StringIO

#¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨#
#           CONSTANTS          #
#______________________________#

API_NOMINATIM = 'https://nominatim.openstreetmap.org/'


#¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨#
#         AWS Settings         #
#______________________________#

import boto3
s3 = boto3.resource('s3')
client = boto3.client('s3')
bucket_name = "kayak-jedha-certification-2023"
object_key = "top_french_cities_spider/35-cities-to-visit-in-france.json"

session = boto3.Session(
    aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY')
)


#¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨#
#           FUNCTIONS          #
#______________________________#

def get_cities_infos(cities: list) -> list:
    cities_infos = []
    if cities:
        for city in cities:
            try:
                # we test if we recover the result at index 0 of our answer for the city request
                response = requests.get(
                    API_NOMINATIM + 'search', params={'city': city, 'country': 'france', 'format': 'json'}).json()[0]
            except IndexError:
                # if we have an exception, we test if we recover the result at index 0 of our answer for the street request
                try:
                    response = requests.get(API_NOMINATIM + 'search', params={
                                            'street': city, 'country': 'france', 'format': 'json'}).json()[0]
                except IndexError:
                    # if we have an exception again, so we pass and set the city gps as ""
                    response = False
                    pass
            if response:
                city_name = response["display_name"].split(',')[0]

                cities_infos.append({
                    "city_name": city_name,
                    "lat": response["lat"],
                    "lon": response["lon"],
                })
            else:
                cities_infos.append({
                    "name": city,
                    "lat": "",
                    "lon": "",
                })
    return cities_infos


#¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨#
#             MAIN             #
#______________________________#
json_obj = client.get_object(Bucket=bucket_name, Key=object_key)
json_string = json.loads(json_obj['Body'].read().decode('utf-8'))
print(f'{object_key} has been import\n\n')
cities = json_string[0]["cities"]

df = pd.DataFrame(data=get_cities_infos(cities))
csv_buffer = StringIO()
df.to_csv(csv_buffer, index=False)

# export to s3 bucket kayak-jedha-certification-2023
file_name = "cities_location.csv"
s3.Object(bucket_name,file_name).put(Body=csv_buffer.getvalue())
print(f"{file_name} has been export to {bucket_name} bucket")
