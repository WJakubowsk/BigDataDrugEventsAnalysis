import json
from hdfs import InsecureClient
import happybase
from typing import Dict, List, Any


def create_hbase_table(
    connection: happybase.Connection, table_name: str, column_families: List[str]
):
    """
    Creates an HBase table for drug events if it does not exist
    """
    connection.create_table(table_name, {cf: dict() for cf in column_families})


def extract_information_from_record(
    record_data: Dict[str, Any], column_mapping: Dict[str, str]
) -> Dict[str, Any]:
    """
    Extracts information from a record and returns a dictionary with the extracted data
    """
    extracted_data = {}
    for col, mapped_col in column_mapping.items():
        current_data = record_data
        try:
            nested_cols = col.split(".")
            for nested_col in nested_cols:
                if isinstance(current_data[nested_col], list):
                    current_data = current_data[nested_col][0]
                else:
                    current_data = current_data[nested_col]
            extracted_data[mapped_col] = current_data
        except (KeyError, TypeError):
            extracted_data[mapped_col] = None
    return extracted_data


def main():
    hdfs_host = "node1"
    hdfs_port = 50070
    hdfs_folder_path = "/user/vagrant/project/nifi_in/api"

    hbase_host = "localhost"
    hbase_table_name = "events"
    column_families = ["report", "patient", "drug"]

    hdfs_client = InsecureClient(f"http://{hdfs_host}:{hdfs_port}")
    connection = happybase.Connection(hbase_host)

    if hbase_table_name.encode() not in connection.tables():
        create_hbase_table(connection, hbase_table_name, column_families)

    table = connection.table(hbase_table_name)

    # Define column mappings
    column_mapping = {
        "safetyreportid": "report:id",
        "receivedate": "report:date",
        "primarysource.reportercountry": "report:country",
        "patient.patientsex": "patient:sex",
        "patient.reaction.reactionmeddrapt": "patient:reaction",
        "patient.patientagegroup": "patient:age_group",
        "patient.patientdeath.patientdeathdate": "patient:death_date",
        "patient.drug.openfda.application_number": "drug:administration_route",
        "patient.drug.medicinalproduct": "drug:medicinal_product_name",
        "patient.drug.openfda.substance_name": "drug:substance_name",
        "patient.drug.openfda.brand_name": "drug:brand_name",
        "patient.drug.openfda.manufacturer_name": "drug:manufacturer_name",
        "patient.drug.openfda.generic_name": "drug:generic_name",
    }

    events_files = hdfs_client.list(hdfs_folder_path)
    for file in events_files:
        file_path = f"{hdfs_folder_path}/{file}"
        with hdfs_client.read(file_path) as reader:
            data = json.load(reader)
            if "results" in data:
                for result in data["results"]:
                    extracted_data = extract_information_from_record(
                        result, column_mapping
                    )
                    row_key = f'{extracted_data["report:id"]}'
                    table.put(row_key, extracted_data)

    connection.close()
    hdfs_client.close()


if __name__ == "__main__":
    main()