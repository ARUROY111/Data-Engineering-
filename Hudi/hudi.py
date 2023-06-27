from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import Row

# Initialize SparkSession
#spark = SparkSession.builder.appName("HudiUpsertExample").getOrCreate()
spark = SparkSession.builder.config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer').config('spark.sql.hive.convertMetastoreParquet', 'false').getOrCreate()

# Define schema for the dataset
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])

# Specify the upsert operation
hudi_options = {
    'hoodie.table.name': 'Age-table',
    'hoodie.datasource.write.recordkey.field': 'id',
    'hoodie.datasource.write.precombine.field': 'age',
    'hoodie.datasource.write.operation': 'upsert',
    'hoodie.upsert.shuffle.parallelism': '2'  # Number of parallelism for upsert
}

# Read the upsert data
upsert_data = [
    Row(id=1, name="John", age=25),
    Row(id=2, name="Jane", age=30),
    Row(id=3, name="Jme", age=36),
    Row(id=4, name="Dame", age=38)
]

df = spark.createDataFrame(spark.sparkContext.parallelize(upsert_data), schema)

# Perform the upsert operation
df.write.format("hudi"). \
    options(**hudi_options). \
    mode("append"). \
    save("s3://webbktt/Hudi /")

# Stop the SparkSession
spark.stop()