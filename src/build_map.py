#¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨#
#           LIBRARIES          #
#______________________________#

import pandas as pd
from io import StringIO
import boto3
import folium
import json

# ¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨#
#           Functions           #
# ______________________________#

def export_csv_to_s3(data: pd.DataFrame, bucket_name , file_name):
    csv_buffer = StringIO()
    data.to_csv(csv_buffer, index=False)
    # export to s3 bucket kayak-jedha-certification-2023
    s3.Object(bucket_name, file_name).put(Body=csv_buffer.getvalue())
    print(f"{file_name} has been export to {bucket_name} bucket")

def import_csv_from_s3(bucket_name , object_key):
    # import cities_infos.csv from s3 bucket
    csv_obj = s3.get_object(Bucket=bucket_name, Key=object_key)
    csv_string = csv_obj['Body'].read().decode('utf-8')
    print(f'{object_key} has been import\n\n')    
    return StringIO(csv_string)

#¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨#
#           Settings           #
#______________________________#
s3 = boto3.client('s3')
bucket_name = "kayak-jedha-certification-2023"
geojson_list = []
France_coord= {
    "lat": 46.603354,
    "lon": 1.8883335
}
m = folium.Map(location=[France_coord["lat"], France_coord["lon"]], zoom_start=7)

#¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨#
#             MAIN             #
#______________________________#

# create dataframe of top-5_destinations
object_key = "top-5_destinations.csv"
df_destinations = pd.read_csv(import_csv_from_s3(bucket_name, object_key))

# create dataframe of hotels from booking.com
object_key = "kayak_dataset.csv"
df_hotels = pd.read_csv(import_csv_from_s3(bucket_name, object_key))
df_hotels.dropna(inplace=True)

# retrive Geojson from s3 bucket
for city in df_destinations['city']:
    json_obj = s3.get_object(Bucket=bucket_name, Key='geojson/'+city)
    json_string = json.loads(json_obj['Body'].read().decode('utf-8'))
    geojson_list.append(json_string)

# add geojson to the map
for geojson in geojson_list:
    folium.GeoJson(geojson, name="cities geojson").add_to(m)

# Add circles that represent the distance from each city center
for row in df_destinations.values:
    folium.Circle(
        radius=5000, location=[row[1],row[2]], color='black',
        popup=folium.Popup(
            f"""
                <h1>5 Kms du centre ville</h1>
            """,
            max_width=500, sticky=False) 
            ).add_to(m)

# Add markers for hotels and some informations inside the popup
for i in range(0,len(df_hotels)):
   folium.Marker(
      location=[df_hotels.iloc[i]['hotel_coord'].split(",")[0], df_hotels.iloc[i]['hotel_coord'].split(",")[1]],
      popup=folium.Popup(
         f"""
            <h1>{df_hotels.iloc[i]['hotel_adress']}</h1><br/>
            <div align="center">
               <a href="{df_hotels.iloc[i]['hotel_url']}" target="blank"><img src="{df_hotels.iloc[i]['hotel_img']}" alt="Visiter la page de l'hôtel"></a>
               <h2>Note : <i class="fa-solid fa-star" style="color: #f1c232"> {df_hotels.iloc[i]['hotel_score']}</i></h2>
               <h2><a href="{df_hotels.iloc[i]['hotel_url']}" target="blank">Réserver une chambre</a></h2>
            </div>
            <ul>
               <h2>Météo <i class="fa-solid fa-cloud-sun"></i></h2>
               <li>Aujourd'hui : {df_hotels.iloc[i]['curr_temp']} °C</li>
               <li>Jour + 1 : {df_hotels.iloc[i]['day+1_temp']} °C</li>
               <li>Jour + 2 : {df_hotels.iloc[i]['day+2_temp']} °C</li>
               <li>Jour + 3 : {df_hotels.iloc[i]['day+3_temp']} °C</li>
               <li>Jour + 4: {df_hotels.iloc[i]['day+4_temp']} °C</li>
               <li>Jour + 5: {df_hotels.iloc[i]['day+5_temp']} °C</li>
               <li>Jour + 6: {df_hotels.iloc[i]['day+6_temp']} °C</li>
               <li>Jour + 7: {df_hotels.iloc[i]['day+7_temp']} °C</li>
            </ul>
            </span>
         """,
         max_width=500, sticky=False),
      icon=folium.Icon(color="red", icon="bed", prefix='fa')
   ).add_to(m)
   
m.save("../map.html")