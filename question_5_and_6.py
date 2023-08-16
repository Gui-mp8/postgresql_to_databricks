# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, masked_email
from pyspark.sql.types import StringType

# Initialize Spark session
spark = SparkSession.builder.appName("EcommerceAnalysis").getOrCreate()

# Read Parquet files into DataFrames
table_names = [
    "customers",
    "employees",
    "offices",
    "orderdetails",
    "orders",
    "payments",
    "product_lines",
    "products",
]

data_frames = {}
for table_name in table_names:
    df = spark.read.parquet(f"/dbfs/path_to_parquet_folder/{table_name}.parquet")
    data_frames[table_name] = df

# Answering questions using Spark SQL
# 5.1: Which country has the highest number of canceled items?
canceled_items_country = data_frames["orders"].filter(col("status") == "Cancelled") \
    .groupBy("country") \
    .count() \
    .orderBy(col("count").desc()) \
    .limit(1)

# 5.2: What is the revenue of the best-selling product line for items shipped in 2005?
best_selling_product_line = data_frames["products"] \
    .join(data_frames["orderdetails"], "productCode") \
    .join(data_frames["orders"], "orderNumber") \
    .filter(col("orderDate").startswith("2005") & (col("status") == "Shipped")) \
    .groupBy("productLine") \
    .agg(col("priceEach") * col("quantityOrdered").alias("revenue")) \
    .orderBy(col("revenue").desc()) \
    .limit(1)

# 5.3: Get masked name, surname, and email of salespeople from Japan
masked_email_udf = spark.udf.register("masked_email", masked_email, StringType())
salespeople_japan = data_frames["employees"] \
    .filter(col("officeCode") == "4") \
    .select(col("firstName"), col("lastName"), masked_email_udf(col("email")).alias("masked_email"))

# Saving results in Delta format
canceled_items_country.write.format("delta").mode("overwrite").save("/dbfs/path_to_delta_folder/canceled_items_country")
best_selling_product_line.write.format("delta").mode("overwrite").save("/dbfs/path_to_delta_folder/best_selling_product_line")
salespeople_japan.write.format("delta").mode("overwrite").save("/dbfs/path_to_delta_folder/salespeople_japan")

# Stopping Spark session
spark.stop()
