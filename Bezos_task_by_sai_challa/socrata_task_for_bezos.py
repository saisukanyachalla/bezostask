from datetime import datetime
from urllib.request import urlretrieve
import boto3
import psycopg2
import os

TS = datetime.now().strftime("%Y%m%d%H%M%S")
URL = "https://data.cityofnewyork.us/api/views/8wbx-tsch/rows.csv?accessType=DOWNLOAD"
BUCKET_NAME = "socrata-task-for-bezos"
FILE_NAME = "socrata_data_" + TS + ".csv"
REDSHIFT_HOST = "host name here"
iam_role = "iam role here"


# download file with url
def get_data_to_file(url, file_name):
    urlretrieve(url, file_name)


# load FILE_NAME to s3 bucket BUCKET_NAME
def load_to_s3_bucket(file_name):
    s3 = boto3.client("s3")

    with open(file_name, "rb") as f:
        s3.upload_fileobj(f, BUCKET_NAME, 'data/{}'.format(file_name))


# load s3 file to redshift
def load_from_s3_to_redshift(file_name):
    conn = psycopg2.connect(dbname='dev', host=REDSHIFT_HOST, port='5439', user=os.environ['USER'], password=os.environ['PASSWORD'])
    cur = conn.cursor()
    cur.execute("begin;")
    cur.execute(
        f"copy socratadata from 's3://{BUCKET_NAME}/data/{file_name}' iam_role '{iam_role}' csv IGNOREHEADER 1 DELIMITER ',';")
    cur.execute("commit;")


print(FILE_NAME)
get_data_to_file(URL, FILE_NAME)
print("download done")
load_to_s3_bucket(FILE_NAME)
print("upload done")
load_from_s3_to_redshift(FILE_NAME)
print("load to redshift done")
