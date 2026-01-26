import os
from dotenv import load_dotenv

from pyspark.sql import DataFrame
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.sql.functions import (
    col, sum, count, avg, stddev, when, to_date, desc, round, date_format
)
# ======================================================
# 0. Carga de info desde la bbdd
# ======================================================
def load_sales_from_postgres(spark, table_name) -> DataFrame:
    """
    Carga los datos de ventas desde PostgreSQL
    """

    jdbc_url = (
        f"jdbc:postgresql://{os.getenv('DB_HOST')}:"
        f"{os.getenv('DB_PORT')}/"
        f"{os.getenv('DB_NAME')}"
    )

    properties = {
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "driver": "org.postgresql.Driver"
    }

    return (
        spark.read
        .jdbc(
            url=jdbc_url,
            table=table_name,
            properties=properties
        )
    )

def load_stores_from_postgres(spark) -> DataFrame:
    return load_sales_from_postgres(spark, "stores")

def load_products_from_postgres(spark) -> DataFrame:
    return load_sales_from_postgres(spark, "products")


# ======================================================
# 1. Carga y Preprocesamiento
# ======================================================
def load_and_prepare_transactions(spark, path):
    """
    Carga el CSV de transacciones y prepara el DataFrame para análisis.
    """

    #df_raw = spark.read.csv(path, header=True, inferSchema=True)
    df_raw = load_sales_from_postgres(spark, "transactions")
    print(f">>> Registros cargados: {df_raw.count()}")

    df_clean = (
        df_raw
        .filter(col("Invoice_ID").isNotNull() & col("Customer_ID").isNotNull())
        .withColumn("Unit_Price", col("Unit_Price").cast(DoubleType()))
        .withColumn("Quantity", col("Quantity").cast(IntegerType()))
    )

    df_processed = df_clean.withColumn(
        "TotalAmount",
        round(col("Unit_Price") * col("Quantity"), 2)
    )

    return df_processed

# ======================================================
# 2. Top de ventas por sucursal
# ======================================================
def top_sales(df: DataFrame, stores_df: DataFrame, top_n: int = 5) -> DataFrame:
    """
    Retorna el top N de sucursales con mayor volumen de ventas.
    """

    return (
        df
        .filter(col("Transaction_Type") == "Sale")
        .groupBy("Store_ID")
        .agg(round(sum("TotalAmount"), 2).alias("Total_Earnings"))
        .join(stores_df, on="Store_ID", how="left")
        .select("Store_ID", "Store_Name", "Total_Earnings")
        .orderBy(desc("Total_Earnings"))
        .limit(top_n)
    )

# ======================================================
# 3. Ventas totales por sucursal
# ======================================================
def total_sales_by_branch(df: DataFrame, stores_df: DataFrame, store_id: int) -> DataFrame:
    """
    Retorna las ventas totales de una sucursal específica.
    """
    return (
        df
        .filter(
            (col("Transaction_Type") == "Sale") &
            (col("Store_ID") == store_id)
        )
        .groupBy("Store_ID")
        .agg(
            round(sum("TotalAmount"), 2).alias("Total_Sales")
        )
        .join(stores_df, on="Store_ID", how="left")
        .select("Store_ID", "Store_Name", "Total_Sales")
    )

# ======================================================
# 4. Productos más vendidos
# ======================================================
def top_selling_products(df: DataFrame, products_df: DataFrame, top_n: int = 10) -> DataFrame:
    """
    Retorna los productos más vendidos según cantidad.
    """
    return (
        df
        .filter(col("Transaction_Type") == "Sale")
        .groupBy("Product_ID")
        .agg(sum("Quantity").alias("Sold_Units"))
        .join(products_df, on="Product_ID", how="left")
        .select("Product_ID", "Description_EN", "Sold_Units")
        .orderBy(desc("Sold_Units"))
        .limit(top_n)
    )


# ======================================================
# 5. Tasa de devoluciones por tienda
# ======================================================
def return_rate_by_store( df: DataFrame, stores_df: DataFrame) -> DataFrame:
    """
    Calcula la tasa de devoluciones por tienda,
    incluyendo el nombre de la sucursal.
    """
    total_tx_store = (
        df
        .groupBy("Store_ID")
        .agg(count("*").alias("Total_Tx"))
    )

    return_tx_store = (
        df
        .filter(col("Transaction_Type") == "Return")
        .groupBy("Store_ID")
        .agg(count("*").alias("Return_Tx"))
    )

    return (
        total_tx_store
        .join(return_tx_store, on="Store_ID", how="left")
        .fillna(0, subset=["Return_Tx"])
        .withColumn(
            "Return_Rate",
            round((col("Return_Tx") / col("Total_Tx")) * 100, 2)
        )
        .join(stores_df, on="Store_ID", how="left")
        .select(
            "Store_ID",
            "Store_Name",
            "Total_Tx",
            "Return_Tx",
            "Return_Rate"
        )
        .orderBy(col("Return_Rate").desc())
    )


# ======================================================
# 6. Días con picos anómalos de devoluciones
# ======================================================
def anomalous_return_days(df: DataFrame) -> DataFrame:
    """
    Detecta días con picos anómalos de devoluciones.
    """

    daily_returns = (
        df
        .filter(col("Transaction_Type") == "Return")
        .groupBy("Date")
        .agg(
            round(sum("TotalAmount"), 2).alias("Refunded_Amount")
        )
    )

    stats = daily_returns.select(
    round(avg("Refunded_Amount"), 2).alias("media"),
    round(stddev("Refunded_Amount"), 2).alias("desviacion")
    ).collect()[0]

    media = stats["media"]
    desviacion = stats["desviacion"]

    umbral = media + desviacion

    anomalies = daily_returns.filter(col("Refunded_Amount") > umbral)
    anomalies = anomalies.withColumn("Date", date_format(col("Date"), "yyyy-MM-dd"))

    return anomalies

# ======================================================
# 7. Top de productos más vendidos por sucursal
# ======================================================
def top_selling_products_by_store(df: DataFrame, products_df: DataFrame, store_id: int, top_n: int = 10) -> DataFrame:
    """
    Retorna el top N de productos más vendidos
    para una sucursal específica.
    """

    return (
        df
        .filter(
            (col("Transaction_Type") == "Sale") &
            (col("Store_ID") == store_id)
        )
        .groupBy("Product_ID")
        .agg(sum("Quantity").alias("Sold_Units"))
        .join(products_df, on="Product_ID", how="left")
        .select("Product_ID", "Description_EN", "Sold_Units")
        .orderBy(desc("Sold_Units"))
        .limit(top_n)
    )