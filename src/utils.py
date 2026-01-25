from pyspark.sql import DataFrame
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.sql.functions import (
    col, sum, count, avg, stddev, when, to_date, desc, round
)

# ======================================================
# 1. Carga y Preprocesamiento
# ======================================================
def load_and_prepare_transactions(spark, path):
    """
    Carga el CSV de transacciones y prepara el DataFrame para análisis.
    """

    df_raw = spark.read.csv(path, header=True, inferSchema=True)
    print(f">>> Registros cargados: {df_raw.count()}")

    df_clean = (
        df_raw
        .filter(col("Invoice ID").isNotNull() & col("Customer ID").isNotNull())
        .withColumn("Unit Price", col("Unit Price").cast(DoubleType()))
        .withColumn("Quantity", col("Quantity").cast(IntegerType()))
    )

    df_processed = df_clean.withColumn(
        "TotalAmount",
        round(col("Unit Price") * col("Quantity"), 2)
    )

    return df_processed

# ======================================================
# 2. Top de ventas por sucursal
# ======================================================
def top_sales(df: DataFrame, top_n: int = 5) -> DataFrame:
    """
    Retorna el top N de sucursales con mayor volumen de ventas.
    """
    return (
        df
        .filter(col("Transaction Type") == "Sale")
        .groupBy("Store ID")
        .agg(round(sum("TotalAmount"), 2).alias("Ingresos_Totales"))
        .orderBy(desc("Ingresos_Totales"))
        .limit(top_n)
    )
# ======================================================
# 3. Ventas totales por sucursal
# ======================================================
def total_sales_by_branch(df: DataFrame, store_id: int) -> DataFrame:
    """
    Retorna las ventas totales de una sucursal específica.
    """
    return (
        df
        .filter(
            (col("Transaction Type") == "Sale") &
            (col("Store ID") == store_id)
        )
        .groupBy("Store ID")
        .agg(
            round(sum("TotalAmount"), 2).alias("Ventas_Totales")
        )
    )

# ======================================================
# 4. Productos más vendidos
# ======================================================
def top_selling_products(df: DataFrame, top_n: int = 10) -> DataFrame:
    """
    Retorna los productos más vendidos según cantidad.
    """
    return (
        df
        .filter(col("Transaction Type") == "Sale")
        .groupBy("Product ID")
        .agg(sum("Quantity").alias("Unidades_Vendidas"))
        .orderBy(desc("Unidades_Vendidas"))
        .limit(top_n)
    )


# ======================================================
# 5. Tasa de devoluciones por tienda
# ======================================================
def return_rate_by_store(df: DataFrame) -> DataFrame:
    """
    Calcula la tasa de devoluciones por tienda.
    Se asume una columna 'is_returned' (1 = devuelto, 0 = no).
    """
    total_tx_store = (
        df
        .groupBy("Store ID")
        .count()
        .withColumnRenamed("count", "Total_Tx")
    )

    failed_tx_store = (
        df
        .filter(col("Transaction Type") == "Return")
        .groupBy("Store ID")
        .count()
        .withColumnRenamed("count", "Failed_Tx")
    )

    return (
        total_tx_store
        .join(failed_tx_store, "Store ID", "left_outer")
        .fillna(0)
        .withColumn("Tasa_Fallo", round((col("Failed_Tx") / col("Total_Tx")) * 100, 2))
    )


# ======================================================
# 6. Días con picos anómalos de devoluciones
# ======================================================
def anomalous_return_days(df: DataFrame, z_threshold: float = 2.0) -> DataFrame:
    """
    Detecta días con picos anómalos de devoluciones usando Z-score.
    """

    daily_returns = (
        df
        .filter(col("Transaction Type") == "Return")
        .groupBy("Date")
        .agg(
            round(sum("TotalAmount"), 2).alias("Monto_Devuelto")
        )
    )

    stats = daily_returns.select(
    round(avg("Monto_Devuelto"), 2).alias("media"),
    round(stddev("Monto_Devuelto"), 2).alias("desviacion")
    ).collect()[0]

    anomalies = daily_returns.filter(col("Monto_Devuelto") > z_threshold)

    return anomalies