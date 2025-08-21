from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

def get_spark_session() -> SparkSession:
    """
    Returns a Spark session configured for Delta Lake.
    """
    builder = SparkSession.builder \
        .appName("Learning Spark") \
        .master("local[*]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.catalog.delta", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.catalogImplementation", "hive") \
        .config("javax.jdo.option.ConnectionURL", "jdbc:derby:;databaseName=metastore_db;create=true")


        # .config("spark.ui.showConsoleProgress", "false") \

    spark = configure_spark_with_delta_pip(builder) \
            .getOrCreate()
    
    return spark