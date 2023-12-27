from pyhive import hive
import pandas as pd
from hdfs import InsecureClient


def read_csv_from_hdfs(hdfs_client: InsecureClient, hdfs_csv_path: str) -> pd.DataFrame:
    """
    Reads a CSV file from HDFS and returns a Pandas DataFrame
    """
    with hdfs_client.read(hdfs_csv_path) as reader:
        df = pd.read_csv(reader, sep=",", keep_default_na=False, na_values=["", None])
    return df


def filter_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filters the DataFrame to only contain the country name and code
    """
    df_filtered = df.copy()
    df_filtered["Name"] = df_filtered["Name"].str.split(",").str[0]
    return df_filtered


def create_hive_table(cursor, hive_table_name: str):
    """
    Creates a Hive table for country codes if it does not exist
    """
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {hive_table_name} (
        country VARCHAR(150),
        code VARCHAR(150)
    )
    """
    cursor.execute("USE default")
    cursor.execute(create_table_query)


def insert_data_into_hive(cursor, hive_table_name: str, df_mapped: pd.DataFrame):
    """
    Inserts data into the Hive table
    """
    columns_str = ", ".join(df_mapped.columns)
    insert_query = f"INSERT INTO {hive_table_name} ({columns_str}) VALUES (%s, %s)"
    rows_to_insert = [tuple(row) for row in df_mapped.values]

    for row in rows_to_insert:
        cursor.execute(insert_query, row)


def main():
    hive_host = "localhost"
    hive_port = 10000
    hdfs_host = "node1"
    hdfs_port = 50070
    hdfs_csv_path = "/user/vagrant/project/nifi_in/country_codes.csv"
    hive_table_name = "country_codes"

    hdfs_client = InsecureClient(f"http://{hdfs_host}:{hdfs_port}", user="vagrant")

    df = read_csv_from_hdfs(hdfs_client, hdfs_csv_path)
    df_filtered = filter_dataframe(df)

    conn = hive.Connection(
        host=hive_host, port=hive_port, auth=None, database="default"
    )
    with conn.cursor() as cursor:
        create_hive_table(cursor, hive_table_name)
        df_mapped = df_filtered.rename(columns={"Name": "country", "Code": "code"})
        insert_data_into_hive(cursor, hive_table_name, df_mapped)

    conn.close()


if __name__ == "__main__":
    main()
