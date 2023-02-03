# ¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨#
#           LIBRARIES          #
# ______________________________#
import boto3
import os
import requests as req
import pandas as pd
from datetime import datetime
from io import StringIO
from tqdm import tqdm

# ¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨#
#           CONSTANTS          #
# ______________________________#

END_PONT = 'https://api.openweathermap.org'
OPEN_WEATHER_API_KEY = os.environ.get("OPEN_WEATHER_API_KEY")
LANG = 'fr'  # to get the output in french
EXCLUDE = 'hourly,minutely,current'  # I just want daily weather
UNIT = 'metric'  # For temperature in Celsius and wind speed in meter/sec

# ¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨#
#         AWS Settings         #
# ______________________________#
s3 = boto3.resource('s3')
client = boto3.client('s3')
bucket_name = "kayak-jedha-certification-2023"
object_key = "cities_location.csv"

session = boto3.Session(
    aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY')
)


# ¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨#
#             MAIN             #
# ______________________________#

# import cities_infos.csv from s3 bucket
csv_obj = client.get_object(Bucket=bucket_name, Key=object_key)
csv_string = csv_obj['Body'].read().decode('utf-8')
print(f'{object_key} has been import\n\n')

# create dataframe of cities
df = pd.read_csv(StringIO(csv_string))

# get weather for all my cities from API
dictionary = {}

for city in tqdm(range(len(df))):
    response = req.get(END_PONT+'/data/2.5/onecall', params={
                       'lat': df['lat'][city], 'lon': df['lon'][city], 'units': UNIT, 'exclude': EXCLUDE, "lang": LANG, 'appid': OPEN_WEATHER_API_KEY})
    dictionary.setdefault('name',[]).append(df['city_name'][city])
    dictionary.setdefault('lat',[]).append(df['lat'][city])
    dictionary.setdefault('lon',[]).append(df['lon'][city])
    dictionary.setdefault('curr_temp',[]).append(response.json()['daily'][0]['feels_like']["day"]) # I choose to work with feels_like temperature
    dictionary.setdefault('curr_weather',[]).append(response.json()['daily'][0]['weather'][0]['main'])

    for (cpt, day) in enumerate(response.json()['daily']):
        if cpt != 0:
            timestamp = day['dt']
            day_name = datetime.fromtimestamp(timestamp).strftime("%A")
            dictionary.setdefault(f'{day_name}_temp',[]).append(day['feels_like']["day"])
            dictionary.setdefault(f'{day_name}_weather',[]).append(day['weather'][0]['main'])
        else:
            continue

df = pd.DataFrame.from_dict(data=dictionary)
csv_buffer = StringIO()
df.to_csv(csv_buffer, index=False)

# export to s3 bucket kayak-jedha-certification-2023
file_name = "cities_weathers.csv"
s3.Object(bucket_name, file_name).put(Body=csv_buffer.getvalue())
print(f"{file_name} has been export to {bucket_name} bucket")
