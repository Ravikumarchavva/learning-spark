from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

def get_spark_session() -> SparkSession:
    """
    Returns a Spark session configured for Delta Lake.
    """
    builder = SparkSession.builder \
        .appName("Learning Spark") \
        .master("local[*]") \
        .config("spark.ui.showConsoleProgress", "false") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.catalog.delta", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder) \
            .getOrCreate()
    
    return spark