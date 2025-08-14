from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Learning Spark") \
    .master("local[*]") \
    .config("spark.ui.showConsoleProgress", "false") \
    .getOrCreate()

textFile = spark.read.text("README.md")

print("Number of lines in README.md:", textFile.count())