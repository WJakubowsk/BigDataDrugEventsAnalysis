import sys
import logging
from pyspark.sql import SparkSession

# Logging configuration
formatter = logging.Formatter('[%(asctime)s] %(levelname)s @ line %(lineno)d: %(message)s')
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
handler.setFormatter(formatter)
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(handler)

def create_spark_session(app_name: str = "VisualizationsTests") -> SparkSession:
    """
    Create a Spark session.

    Args:
        app_name (str): The name of the Spark application.

    Returns:
        SparkSession: The Spark session.
    """
    return SparkSession.builder.appName(app_name).enableHiveSupport().getOrCreate()

def main():
    # start Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    logger.info("Starting spark application")

    # read tables
    logger.info("Reading merged table")
    merged_df = spark.read.parquet('hdfs://localhost:8020/user/vagrant/project/BigDataProject/data/merged.parquet')
    merged_df.show()

    # test aggregations
    merged_df.createOrReplaceTempView("results")

    logger.info("Cards visualization test")
    events_count = spark.sql("SELECT COUNT(*) as events_count FROM results")
    deaths_count = spark.sql("SELECT COUNT(*) as deaths_count FROM results WHERE reaction = 'Death'")
    hosp_count = spark.sql("SELECT COUNT(*) as hosp_count FROM results WHERE reaction = 'Hospitalisation'")
    events_count.show()
    deaths_count.show()
    hosp_count.show()


    logger.info("Events by Sex visualization test")
    gender_dist = spark.sql("SELECT sex, COUNT(*) as count FROM results WHERE sex != 'Unknown' GROUP BY sex")
    gender_dist.show()

    logger.info("Events by Age Group visualization test")
    age_dist = spark.sql("SELECT age_group, COUNT(*) as count FROM results WHERE age_group != 'Unknown' GROUP BY age_group")
    age_dist.show()

    logger.info("Events by Country visualization test")
    country_dist = spark.sql("SELECT country_name, COUNT(*) as count FROM results GROUP BY country_name ORDER BY count DESC")
    country_dist.show()

    logger.info("Events by Year and Month visualization test")
    monthly_count = spark.sql("SELECT year(date) as Year, month(date) as Month, COUNT(*) as count FROM results GROUP BY Year, Month ORDER BY Month")
    monthly_count.show()

    logger.info("Top 5 drugs reported visualization test")
    top_drugs = spark.sql("SELECT DrugName, COUNT(*) as count FROM results WHERE DrugName is not null GROUP BY DrugName ORDER BY count DESC LIMIT 5")
    top_drugs.show()

    logger.info("Top reactions visualization test")
    top_reactions = spark.sql("SELECT reaction, COUNT(*) as count FROM results GROUP BY reaction ORDER BY count DESC LIMIT 5")
    top_reactions.show()

    logger.info("Top 10 drugs with death effects visualization test")
    top_death = spark.sql("SELECT DrugName, COUNT(*) as count FROM results WHERE (DrugName is not null AND reaction = 'Death') GROUP BY DrugName ORDER BY count DESC LIMIT 10")
    top_death.show()

    logger.info("Death cases by Year, Month and Day visualization test")
    time_death = spark.sql("SELECT year(date) as Year, month(date) as Month, day(date) as Day, COUNT(*) as count FROM results WHERE reaction = 'Death' GROUP BY Year, Month, Day ORDER BY Year, Month, Day")
    time_death.show()

    logger.info("Death Cases by Manufacturer visualization test")
    man_death = spark.sql("SELECT manufacturer_name, COUNT(*) as count FROM results WHERE (manufacturer_name is not null AND ActiveIngredient is not null and reaction = 'Death') GROUP BY manufacturer_name ORDER BY count DESC LIMIT 10")
    man_death.show()



if __name__ == "__main__":
    main()