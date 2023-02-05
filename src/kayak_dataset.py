# ¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨#
#           LIBRARIES          #
# ______________________________#
import boto3
import os
import pandas as pd
import numpy as np
from datetime import datetime
from io import StringIO

# ¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨#
#           Functions           #
# ______________________________#

def export_to_s3(data: pd.DataFrame, bucket_name , file_name):
    csv_buffer = StringIO()
    data.to_csv(csv_buffer, index=False)
    # export to s3 bucket kayak-jedha-certification-2023
    s3.Object(bucket_name, file_name).put(Body=csv_buffer.getvalue())
    print(f"{file_name} has been export to {bucket_name} bucket")

def import_from_s3(bucket_name , object_key):
    # import cities_infos.csv from s3 bucket
    csv_obj = client.get_object(Bucket=bucket_name, Key=object_key)
    csv_string = csv_obj['Body'].read().decode('utf-8')
    print(f'{object_key} has been import\n\n')    
    return StringIO(csv_string)


#¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨#
#         AWS Settings         #
#______________________________#

s3 = boto3.resource('s3')
client = boto3.client('s3')
bucket_name = "kayak-jedha-certification-2023"

# create dataframe of top-5_destinations
object_key = "top-5_destinations.csv"
df_destinations = pd.read_csv(import_from_s3(bucket_name, object_key))

# create dataframe of hotels from booking.com
object_key = "booking_hotels_spider/hotel_list.csv"
df_hotels = pd.read_csv(import_from_s3(bucket_name, object_key))

kayak_df = pd.merge(df_hotels, df_destinations, on="city")

export_to_s3(data=kayak_df, bucket_name=bucket_name , file_name='kayak_dataset.csv')