# ¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨#
#           LIBRARIES          #
# ______________________________#
import boto3
import os
import requests as req
import pandas as pd
import numpy as np
from datetime import datetime
from io import StringIO
from tqdm import tqdm
import collections


# ¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨#
#           CONSTANTS           #
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

# ¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨#
#           Functions           #
# ______________________________#

def export_csv_to_s3(data: pd.DataFrame, bucket_name , file_name):
    csv_buffer = StringIO()
    data.to_csv(csv_buffer, index=False)
    # export to s3 bucket kayak-jedha-certification-2023
    s3.Object(bucket_name, file_name).put(Body=csv_buffer.getvalue())
    print(f"{file_name} has been export to {bucket_name} bucket")


# ¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨#
#             MAIN              #
# ______________________________#

# import cities_infos.csv from s3 bucket
csv_obj = client.get_object(Bucket=bucket_name, Key=object_key)
csv_string = csv_obj['Body'].read().decode('utf-8')
print(f'{object_key} has been import\n\n')

# create dataframe of cities
df = pd.read_csv(StringIO(csv_string))

# get weather for all my cities from API
dictionary = {}

for city in tqdm(range(len(df)), desc="API call :"):
    response = req.get(END_PONT+'/data/2.5/onecall', params={
                       'lat': df['lat'][city], 'lon': df['lon'][city], 'units': UNIT, 'exclude': EXCLUDE, "lang": LANG, 'appid': OPEN_WEATHER_API_KEY})
    dictionary.setdefault('city',[]).append(df['city_name'][city])
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

csv_buffer = StringIO()
df.to_csv(csv_buffer, index=False)

export_csv_to_s3(data=df, bucket_name=bucket_name, file_name = "cities_weathers.csv")

# ¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨#
#   Preparing data for export   #
# ______________________________#

# weight dictionary to sort the datas
main_weather_weight = {
    "Clear": 1,
    "Clouds": 2,
    "Fog": 3,
    "Mist": 3,
    "Drizzle": 4,
    "Rain": 4,
    "Snow": 5,
    "Thunderstorm": 6,
    "Haze": 7,
    "Dust": 7,
    "Smoke": 7,
    "Ash": 7,
    "Sand": 7,
    "Squall": 7,
    "Tornado": 8
}

temp_cols = []
weather_cols = []
avg_temp = []
main_weather = []  # the weather that comes up most often
mode_weather = []  # number of time the weather comes up during the week

df = pd.DataFrame.from_dict(data=dictionary)

for col in tqdm(df.columns, desc="temp_cols and weather_cols building :"):
    if "_temp" in col:
        temp_cols.append(col)
    elif "_weather" in col:
        weather_cols.append(col)

for index, row in tqdm(df.iterrows(), desc="[avg_temp, main_weather building, mode_weather] building :"):
    avg_temp.append(round(np.mean(row[temp_cols]), 2))
    main_weather.append(collections.Counter(row[weather_cols]).most_common()[0][0])
    mode_weather.append(collections.Counter(row[weather_cols]).most_common()[0][1])

df['avg_temp'] = avg_temp
df['mode_weather'] = mode_weather
df['main_weather'] = main_weather
df['main_weather_weight'] = df['main_weather'].apply(lambda weather: main_weather_weight[weather])

df.sort_values(by=['main_weather_weight', 'avg_temp'], axis=0, ascending=[True, False], inplace=True, kind='quicksort', ignore_index=True, key=None)

export_csv_to_s3(data=df.head(5), bucket_name=bucket_name, file_name = "top-5_destinations.csv")
