from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, sum, count, avg, stddev, when, to_date
)

# ======================================================
# 1. Top de ventas por sucursal
# ======================================================
def top_sales_by_branch(df: DataFrame, top_n: int = 5) -> DataFrame:
    """
    Retorna el top N de sucursales con mayor volumen de ventas.
    """
    return (
        df.groupBy("branch")
          .agg(sum("total_amount").alias("total_sales"))
          .orderBy(col("total_sales").desc())
          .limit(top_n)
    )


# ======================================================
# 2. Productos más vendidos
# ======================================================
def top_selling_products(df: DataFrame, top_n: int = 10) -> DataFrame:
    """
    Retorna los productos más vendidos según cantidad.
    """
    return (
        df.groupBy("product")
          .agg(sum("quantity").alias("units_sold"))
          .orderBy(col("units_sold").desc())
          .limit(top_n)
    )


# ======================================================
# 3. Tasa de devoluciones por tienda
# ======================================================
def return_rate_by_store(df: DataFrame) -> DataFrame:
    """
    Calcula la tasa de devoluciones por tienda.
    Se asume una columna 'is_returned' (1 = devuelto, 0 = no).
    """
    return (
        df.groupBy("store")
          .agg(
              count("*").alias("total_transactions"),
              sum("is_returned").alias("total_returns")
          )
          .withColumn(
              "return_rate",
              col("total_returns") / col("total_transactions")
          )
          .orderBy(col("return_rate").desc())
    )


# ======================================================
# 4. Días con picos anómalos de devoluciones
# ======================================================
def anomalous_return_days(df: DataFrame, z_threshold: float = 2.0) -> DataFrame:
    """
    Detecta días con picos anómalos de devoluciones usando Z-score.
    """
    daily_returns = (
        df.filter(col("is_returned") == 1)
          .withColumn("date", to_date(col("date")))
          .groupBy("date")
          .agg(count("*").alias("returns_per_day"))
    )

    stats = daily_returns.agg(
        avg("returns_per_day").alias("mean"),
        stddev("returns_per_day").alias("std")
    ).collect()[0]

    mean_val = stats["mean"]
    std_val = stats["std"]

    return (
        daily_returns.withColumn(
            "z_score",
            (col("returns_per_day") - mean_val) / std_val
        )
        .filter(col("z_score").abs() > z_threshold)
        .orderBy(col("z_score").desc())
    )
