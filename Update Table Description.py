from msilib.schema import tables
import os
import time
import datetime as dt
# from google.cloud import bigquery
# client = bigquery.Client()
from google.cloud import bigquery

client = bigquery.Client()


assert tables.description == "Original description."
tables.description = "Updated description."

tables = client.update_table(tables, ["description"])  # API request

assert tables.description == "Updated description."

if__name__='__main__'
print("Awda")
