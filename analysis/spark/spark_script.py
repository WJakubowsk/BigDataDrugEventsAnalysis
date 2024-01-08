import sys
import logging
import happybase
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import regexp_extract, col, when, year, month, to_date

# Logging configuration
formatter = logging.Formatter('[%(asctime)s] %(levelname)s @ line %(lineno)d: %(message)s')
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
handler.setFormatter(formatter)
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(handler)


def read_hbase_table(hbase_host: str, table_name: str):
    """
    Read data from HBase table.

    Args:
        hbase_host (str): The HBase host.
        table_name (str): The name of the HBase table.

    Returns:
        list: List of dictionaries containing the table data.
    """
    connection = happybase.Connection(hbase_host)
    table = connection.table(table_name)
    data_list = []

    for key, data in table.scan():
        key_decoded = key.decode('utf-8')
        row_dict = {'id': key_decoded}

        for column, value in data.items():
            column_decoded = column.decode('utf-8').split(":")[1]
            value_decoded = value.decode('utf-8')
            row_dict[column_decoded] = value_decoded

        data_list.append(row_dict)

    return data_list


def create_spark_session(app_name: str = "DrugEventsAnalysis") -> SparkSession:
    """
    Create a Spark session.

    Args:
        app_name (str): The name of the Spark application.

    Returns:
        SparkSession: The Spark session.
    """
    return SparkSession.builder.appName(app_name).enableHiveSupport().getOrCreate()


def preprocess_events(events_df):
    """
    Preprocess the 'events' DataFrame.

    Args:
        events_df (DataFrame): The 'events' DataFrame.

    Returns:
        DataFrame: The preprocessed 'events' DataFrame.
    """
    events_df = events_df.withColumn("application_number", regexp_extract(events_df["administration_route"], r'\d+', 0).cast("int"))
    events_df = events_df.withColumn("date", to_date(events_df["date"], "yyyyMMdd"))
    events_df = events_df.withColumn("sex", when(col("sex") == "1", "Male").when(col("sex") == "2", "Female").otherwise("Unknown"))
    events_df = events_df.withColumn("age_group",
                                     when(col("age_group") == "1", "Neonate")
                                     .when(col("age_group") == "2", "Infant")
                                     .when(col("age_group") == "3", "Child")
                                     .when(col("age_group") == "4", "Adolescent")
                                     .when(col("age_group") == "5", "Adult")
                                     .when(col("age_group") == "6", "Elderly")
                                     .otherwise("Unknown"))

    return events_df


def preprocess_country_codes(country_codes_df):
    """
    Preprocess the 'country_codes' DataFrame.

    Args:
        country_codes_df (DataFrame): The 'country_codes' DataFrame.

    Returns:
        DataFrame: The preprocessed 'country_codes' DataFrame.
    """
    country_codes_df = country_codes_df.withColumnRenamed("country", "country_name")
    country_codes_df = country_codes_df.withColumnRenamed("code", "country")

    return country_codes_df


def merge_data(events_df, country_codes_df, drugs_df):
    """
    Merge 'events', 'country_codes', and 'drugs' DataFrames.

    Args:
        events_df (DataFrame): The 'events' DataFrame.
        country_codes_df (DataFrame): The 'country_codes' DataFrame.
        drugs_df (DataFrame): The 'drugs' DataFrame.

    Returns:
        DataFrame: Merged DataFrame.
    """
    merged_df = events_df \
        .join(country_codes_df, on="country", how="left") \
        .join(drugs_df, on=events_df["application_number"] == drugs_df["application_number"], how="left") \
        .drop(drugs_df["application_number"])

    return merged_df


def main():
    # start Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    logger.info("Starting spark application")

    # read tables
    logger.info("Reading table 'events'")
    hbase_host = 'localhost'
    hbase_table_name = 'events'
    events_data = read_hbase_table(hbase_host, hbase_table_name)

    schema = StructType([
        StructField("id", StringType(), True),
        StructField("administration_route", StringType(), True),
        StructField("brand_name", StringType(), True),
        StructField("generic_name", StringType(), True),
        StructField("manufacturer_name", StringType(), True),
        StructField("medicinal_product_name", StringType(), True),
        StructField("substance_name", StringType(), True),
        StructField("age_group", StringType(), True),
        StructField("death_date", StringType(), True),
        StructField("reaction", StringType(), True),
        StructField("sex", StringType(), True),
        StructField("country", StringType(), True),
        StructField("date", StringType(), True)
    ])

    events_df = spark.createDataFrame(events_data, schema)

    logger.info("Reading table 'country_codes'")
    country_codes_df = spark.sql("select * from country_codes")

    logger.info("Reading table 'drugs'")
    drugs_df = spark.sql("select * from drugs")

    # show basic info
    logger.info("Previewing 'events'")
    events_df.show()
    print("Number of rows in 'events': ", events_df.count())

    logger.info("Previewing 'country_codes'")
    country_codes_df.show()
    print("Number of rows in 'country_codes': ", country_codes_df.count())

    logger.info("Previewing 'drugs'")
    drugs_df.show()
    print("Number of rows in 'drugs': ", drugs_df.count())


    # preprocess tables
    logger.info("Preprocessing 'events'")
    events_df = preprocess_events(events_df)

    logger.info("Preprocessing 'country_codes'")
    country_codes_df = preprocess_country_codes(country_codes_df)
    country_codes_df.show()

    # merge data
    logger.info("Data merging")
    merged_df = merge_data(events_df, country_codes_df, drugs_df)

    logger.info("Previewing merged data")
    merged_df.show()
    print("Number of rows in merged df: ", merged_df.count())

    # parquet conversion
    logger.info("Parquet conversion")
    merged_df.write.format("parquet").mode("overwrite").save("/user/vagrant/project/BigDataProject/data/merged.parquet")

    
if __name__ == "__main__":
    main()
