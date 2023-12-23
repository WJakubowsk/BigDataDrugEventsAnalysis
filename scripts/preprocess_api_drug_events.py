import json
from hdfs import InsecureClient
import happybase

def create_hbase_table(connection, table_name, column_families):
    connection.create_table(
        table_name,
        {cf: dict() for cf in column_families}
    )


def load_and_concat_json(client, folder_path):
    concatenated_data = []
    file_statuses = client.list(folder_path)
    for file_status in file_statuses:
        file_path = folder_path + '/' + file_status['pathSuffix']
        with client.read(file_path) as reader:
            data = json.load(reader)
            if 'error' not in data:
                if 'results' in data:
                    concatenated_data.extend(data['results'])
    
    return concatenated_data


hdfs_host = 'node1'
hdfs_port = 50070
folder_path = '/user/vagrant/project/nifi_in/api' 

hbase_host = 'localhost'
hbase_table_name = 'events'
column_families = ['description', 'patient', 'drug']

description_columns = [
    'report_id', 'report_date', 'reporter_country'
]

patient_columns = [
    'sex', 'reaction', 'age', 'weight',  'death', 'death_date'
]

drug_columns = [
    'characterization_id', 'medicinal_product_name', 'auth_number', 'administration_route',
    'brand_name', 'substance_name', 'application_number', 'manufacturer_name', 'generic_name'
]

# stworzenie tabeli
column_families = {
    'description': {'max_versions': 1},
    'patient': {'max_versions': 1},
    'drug': {'max_versions': 1}
}

if hbase_table_name.encode() not in connection.tables():
    create_hbase_table(connection, hbase_table_name, column_families)

hdfs_client = InsecureClient(f'http://{hdfs_host}:{hdfs_port}')

connection = happybase.Connection(hbase_host)
table = connection.table(table_name)

json_data = load_and_concat_json(hdfs_client, folder_path)

for idx, row in enumerate(json_data, start=1):
    row_key = f'{idx}'
    description_data = {f'race:{col}': str(row[col]).encode() for col in race_columns if col in df.columns}

    table.put(row_key, row)

connection.close()
hdfs_client.close()


