from pyspark.sql import SparkSession

from utils import (
    total_sales_by_branch,
    top_sales,
    top_selling_products,
    return_rate_by_store,
    anomalous_return_days,
    load_and_prepare_transactions
)

spark = SparkSession.builder \
    .appName("ProcesamientoClothingRetail") \
    .master("local[*]") \
    .config("spark.ui.port", "4040") \
    .config("spark.ui.bindAddress", "0.0.0.0") \
    .config("spark.driver.bindAddress", "0.0.0.0") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.executor.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

df = load_and_prepare_transactions(spark, path = "./data/transactions.csv")

top_sales = top_sales(df)
sales_branch = total_sales_by_branch(df,3)
top_products = top_selling_products(df)
return_rates = return_rate_by_store(df)
anomalies = anomalous_return_days(df)

top_sales.show()
sales_branch.show()
top_products.show()
return_rates.show()
anomalies.show()

spark.stop()
