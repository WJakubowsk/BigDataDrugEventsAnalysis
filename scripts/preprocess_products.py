from pyhive import hive
import pandas as pd
from hdfs import InsecureClient

# Set up connection details
hive_host = 'localhost'
hive_port = 10000
hdfs_host = 'node1'
hdfs_port = 50070
hdfs_csv_path = '/user/vagrant/project/nifi_in/Products.csv'
hdfs_preprocessed_csv_path = '/user/vagrant/project/nifi_in/preprocessed_Drugs.csv'
local_preprocessed_csv_path = '/home/vagrant/project/BigDataProject/data/preprocessed_Drugs.csv'
hive_table_name = 'Drugs'

hdfs_client = InsecureClient(f'http://{hdfs_host}:{hdfs_port}', user = "vagrant")

with hdfs_client.read(hdfs_csv_path) as reader:
    df = pd.read_csv(reader, sep=";")

df_filtered = df.sort_values('ProductNo').drop_duplicates(subset = 'ApplNo', keep='first')

df_filtered.to_csv(local_preprocessed_csv_path, index = False)

# hdfs_client.upload('/user/vagrant/project/nifi_in/', local_preprocessed_csv_path)

# with hdfs_client.write(hdfs_preprocessed_csv_path, encoding='utf-8') as writer:
    # writer.write(df_filtered)
    

conn = hive.Connection(host=hive_host, port=hive_port, auth='NOSASL', database='default')
cursor = conn.cursor()

create_table_query = f'''
CREATE TABLE IF NOT EXISTS {hive_table_name} (
    application_number INT,
    product_number INT,
    drug_administration_form STRING,
    drug_strength STRING,
    reference_drug INT,
    drug_name STRING,
    active_ingredient STRING,
    reference_standard INT
)
'''
cursor.execute(create_table_query)

table_name_with_db = f'default.{hive_table_name}'
cursor.execute(f"USE default")
cursor.execute(f"DROP TABLE IF EXISTS {table_name_with_db}")
cursor.execute(f"CREATE TABLE {table_name_with_db} (LIKE {hive_table_name})")
cursor.execute(f"INSERT INTO TABLE {table_name_with_db} SELECT * FROM {hive_table_name}")

cursor.close()
conn.close()

print("Data loaded into Hive table successfully!")