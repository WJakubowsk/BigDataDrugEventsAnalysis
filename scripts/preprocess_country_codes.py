from pyhive import hive
import pandas as pd
from hdfs import InsecureClient

# Set up connection details
hive_host = 'localhost'
hive_port = 10000
hdfs_host = 'node1'
hdfs_port = 50070
hdfs_csv_path = '/user/vagrant/project/nifi_in/country_codes.csv'
hive_table_name = 'country_codes'

hdfs_client = InsecureClient(f'http://{hdfs_host}:{hdfs_port}', user = "vagrant")

with hdfs_client.read(hdfs_csv_path) as reader:
    df = pd.read_csv(reader, sep=",", keep_default_na=False, na_values=['', None])

df_filtered = df.copy()    
df_filtered['Name'] = df_filtered['Name'].str.split(',').str[0]

conn = hive.Connection(host=hive_host, port=hive_port, auth=None, database='default')
cursor = conn.cursor()

create_table_query = f'''
CREATE TABLE IF NOT EXISTS {hive_table_name} (
    country VARCHAR(150),
    code VARCHAR(150)
)
'''

cursor.execute("USE default")
cursor.execute(create_table_query)

df_mapped = df_filtered.rename(columns={
    'Name': 'country',
    'Code': 'code'
    })

columns_str = ', '.join(df_mapped.columns)


insert_query = f"INSERT INTO {hive_table_name} ({columns_str}) VALUES (%s, %s)"


rows_to_insert = [tuple(row) for row in df_mapped.values]

for row in rows_to_insert:
    cursor.execute(insert_query, row)

conn.commit()
cursor.close()
conn.close()
