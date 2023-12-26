import json
from hdfs import InsecureClient
import happybase
from typing import List, Dict

def create_hbase_table(connection, table_name, column_families):
    connection.create_table(
        table_name,
        {cf: dict() for cf in column_families}
    )
    
    return concatenated_data

def map_column_to_nested_dict(column: str):
    keys = column.split('.')
    nested_dict = {}
    current_dict = nested_dict

    for key in keys:
        current_dict[key] = {}
        current_dict = current_dict[key]

    return nested_dict

def extract_information_from_record(record_data: Dict, columnmapping: Dict[str, str]):
    extracted_data = {}
    for col, mapped_col in column_mapping:
        current_data = result
        try:
            nested_cols = col.split('.')
            for nested_col in nested_cols:
                if current_data[nested_col] isinstance(current_data, list):
                    current_data = current_data[nested_col][0] 
                else:
                    current_data = current_data[nested_col]
            extracted_data[mapped_col] = current_data
        except (KeyError, TypeError):
            extracted_data[mapped_col] = None
    return extracted_data

hdfs_host = 'node1'
hdfs_port = 50070
hdfs_folder_path = '/user/vagrant/project/nifi_in/api' 

hbase_host = 'localhost'
hbase_table_name = 'events'
column_families = ['report', 'patient', 'drug']

hdfs_client = InsecureClient(f'http://{hdfs_host}:{hdfs_port}')

connection = happybase.Connection(hbase_host)
print('created connection')
if hbase_table_name not in connection.tables():
    create_hbase_table(connection, hbase_table_name, column_families)
print('created table')

column_mapping = {
    'safetyreportid': 'report:id',
    'receivedate': 'report:date',
    'primarysource.reportercountry': 'report:country',
    'patient.patientsex': 'patient:sex',
    'patient.reaction.reactionmeddrapt': 'patient:reaction',
    'patient.patientagegroup': 'patient:age_group',
    'patient.patientdeath.patientdeathdate': 'patient:death_date',
    'patient.drug.openfda.application_number': 'drug:administration_route',
    'patient.drug.medicinalproduct': 'drug:medicinal_product_name',
    'patient.drug.openfda.substance_name': 'drug:substance_name',
    'patient.drug.openfda.brand_name': 'drug:brand_name',
    'patient.drug.openfda.manufacturer_name': 'drug:manufacturer_name',
    'patient.drug.openfda.generic_name': 'drug:generic_name'
    }


idx = 0
events_files = client.list(hdfs_folder_path)
for file in events_files:
    file_path = folder_path + '/' + file
    with client.read(file_path) as reader:
        print(f'loading data from {file}')
        data = json.load(reader)
        if 'results' in data:
            for result in data['results']:
                row_key = f'{idx}'
                extracted_data = extract_information_from_record(result, column_mapping)
                table.put(row_key, extracted_data)
                idx += 1

connection.close()
hdfs_client.close()


