from pyhive import hive
import pandas as pd
from hdfs import InsecureClient

hive_host = 'localhost'
hive_port = 10000
hdfs_host = 'node1'
hdfs_port = 50070
hdfs_csv_path = '/user/vagrant/project/nifi_in/Products.csv'
hdfs_preprocessed_csv_path = '/user/vagrant/project/nifi_in/preprocessed_drugs.csv'
local_preprocessed_csv_path = '/home/vagrant/project/BigDataProject/data/preprocessed_drugs.csv'
hive_table_name = 'drugs'

hdfs_client = InsecureClient(f'http://{hdfs_host}:{hdfs_port}', user = "vagrant")

with hdfs_client.read(hdfs_csv_path) as reader:
    df = pd.read_csv(reader, sep=";")

df_filtered = df.sort_values('ProductNo').drop_duplicates(subset = 'ApplNo', keep='first')
df_filtered['Form'] = df_filtered['Form'].str.replace(',', ';')
df_filtered['DrugName'] = df_filtered['DrugName'].str.replace(',', ';')

df_filtered.to_csv(local_preprocessed_csv_path, index = False)

hdfs_client.upload(hdfs_preprocessed_csv_path, local_preprocessed_csv_path)

conn = hive.Connection(host=hive_host, port=hive_port, auth=None, database='default')
cursor = conn.cursor()

create_table_query = f'''
CREATE TABLE IF NOT EXISTS {hive_table_name} (
    application_number INT,
    product_number INT,
    drug_administration_form VARCHAR(100),
    drug_strength VARCHAR(100),
    drug_name VARCHAR(100),
    active_ingredient VARCHAR(100)
)
'''
cursor.execute("USE default")
cursor.execute(create_table_query)

df_mapped = df_filtered.rename(columns={
    'ApplNo': 'application_number',
    'ProductNo': 'product_number',
    'Form': 'drug_administration_form',
    'Strength': 'drug_strength',
    'DrugName': 'drug_name',
    'ActiveIngredient': 'active_ingredient',
    }).drop(['ReferenceDrug', 'ReferenceStandard'], axis = 1)

str_cols = ['drug_administration_form', 'drug_strength', 'drug_name', 'active_ingredient']
int_cols = ['application_number', 'product_number']

df_mapped[str_cols] = df_mapped[str_cols].fillna(value='missing')
df_mapped[int_cols] = df_mapped[int_cols].fillna(value=-1) # application and product numbers are poisitve


columns_str = ', '.join(df_mapped.columns)

placeholders_str = ', '.join(['%s'] * len(df_mapped.columns))

insert_query = f"INSERT INTO {hive_table_name} ({columns_str}) VALUES ({placeholders_str})"

rows_to_insert = [tuple(row) for row in df_mapped.values]

for row in rows_to_insert:
    cursor.execute(insert_query, row)

conn.commit()
cursor.close()
conn.close()
