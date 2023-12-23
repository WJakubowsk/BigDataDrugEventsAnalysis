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
df_filtered['DrugName'] = df_filtered['DrugName'].str.replace(',', ';')

df_filtered.to_csv(local_preprocessed_csv_path, index = False)

# hdfs_client.upload('/user/vagrant/project/nifi_in/', local_preprocessed_csv_path)

# with hdfs_client.write(hdfs_preprocessed_csv_path, encoding='utf-8') as writer:
    # writer.write(df_filtered)
    

conn = hive.Connection(host=hive_host, port=hive_port, auth=None, database='default')
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
cursor.execute("USE default")
cursor.execute(create_table_query)

df_mapped = df_filtered.rename(columns={
    'ApplNo': 'application_number',
    'ProductNo': 'product_number',
    'Form': 'drug_administration_form',
    'Strength': 'drug_strength',
    'ReferenceDrug': 'reference_drug',
    'DrugName': 'drug_name',
    'ActiveIngredient': 'active_ingredient',
    'ReferenceStandard': 'reference_standard'
    })

# df_mapped.fillna(value='NULL', inplace=True) #TODOOO missing values -> NULL, nie nan/0

str_cols = ['drug_administration_form', 'drug_strength', 'drug_name', 'active_ingredient']
int_cols = ['application_number', 'product_number', 'reference_drug', 'reference_standard']
for col in df_mapped.columns:
    format = str if col in str_cols else int
    df_mapped.loc[df_mapped[col].notna(), col] = df_mapped.loc[df_mapped[col].notna(), col].astype(format)


columns_str = ', '.join(df_mapped.columns)
placeholders_str = ', '.join(['?'] * len(df_mapped.columns))  # Using '?' as placeholders for values

# Prepare the INSERT INTO query with explicit column names
insert_query = f"INSERT INTO {hive_table_name} ({columns_str}) VALUES ({placeholders_str})"

# Convert DataFrame rows to tuples for insertion
rows_to_insert = [tuple(row) for row in df_mapped.values]

# Execute insertion query for each row
for row in rows_to_insert:
    cursor.execute(insert_query, row)

conn.commit()
cursor.close()
conn.close()

print("Data loaded into Hive table successfully!")