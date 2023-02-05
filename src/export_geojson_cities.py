# ¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨#
#           LIBRARIES           #
# ______________________________#
import requests
import pandas as pd
import json
from io import StringIO
import boto3

# ¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨#
#         AWS Settings         #
# ______________________________#

s3 = boto3.client('s3')
bucket_name = "kayak-jedha-certification-2023"

# ¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨#
#           CONSTANTS           #
# ______________________________#
API_GEO = 'https://geo.api.gouv.fr/'
CITIES = 'communes'
GEOJSON = '../docs/geojson/'
DOCS = '../docs/'
France_coord= {
    "lat": 46.603354,
    "lon": 1.8883335
}

# ¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨#
#           Functions           #
# ______________________________#

def import_csv_from_s3(bucket_name , object_key):
    # import cities_infos.csv from s3 bucket
    csv_obj = s3.get_object(Bucket=bucket_name, Key=object_key)
    csv_string = csv_obj['Body'].read().decode('utf-8')
    print(f'{object_key} has been import\n\n')    
    return StringIO(csv_string)

def export_json_to_s3(dictionary:dict, bucket_name:str, object_key:str):
    s3.put_object(Body=json.dumps(dictionary), Bucket=bucket_name, Key=object_key)

def get_geojson(lat: float, lon: float):
    return requests.get(API_GEO + CITIES, params={'lat': lat, 'lon': lon, 'fields': ['contour'], 'format': 'geojson', 'geometry': 'contour'}).json()

# ¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨#
#             MAIN              #
# ______________________________#

# create dataframe of top-5_destinations
object_key = "top-5_destinations.csv"
df_destinations = pd.read_csv(import_csv_from_s3(bucket_name, object_key))

geojson_list = []

for count, city in enumerate(df.values):
    #pprint(get_feature(city[1], city[2])['features'])
    city_geojson = get_geojson(city[1], city[2])['features'][0]
    geojson_list.append(city_geojson)
    export_json_to_s3(dictionary=city_geojson,
                      bucket_name=bucket_name, object_key=f'geojson/{city[0]}')
