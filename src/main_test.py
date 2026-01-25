# Librerías utilizadas
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, desc, avg, stddev
from pyspark.sql.types import DoubleType, IntegerType

def main():
    # ---------------------------------------------------------
    # 1. Configuración del Entorno Spark
    # ---------------------------------------------------------
    spark = (
        SparkSession.builder
        .appName("ProcesamientoClothingRetail")
        #.master("local[*]")
        .master("local[2]")
        #.config("spark.ui.port", "4040")
        #.config("spark.ui.bindAddress", "0.0.0.0")
        #.config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.executor.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # Mostrar URL del Spark UI
    #ui_url = spark.sparkContext.uiWebUrl
    #print("\n>>> Entorno Spark iniciado")
    #print(f">>> Spark UI URL (desde la VM): {ui_url}")
    #print(">>> Si estás en tu PC, abre: http://IP_DE_LA_VM:4040\n")

    # ---------------------------------------------------------
    # 2. Carga y Preparación de Datos
    # ---------------------------------------------------------
    path = "../data/transactions.csv"

    df_raw = spark.read.csv(path, header=True, inferSchema=True)
    print(f">>> Registros cargados: {df_raw.count()}")

    df_clean = (
        df_raw.filter(col("Invoice ID").isNotNull() & col("Customer ID").isNotNull())
        .withColumn("Unit Price", col("Unit Price").cast(DoubleType()))
        .withColumn("Quantity", col("Quantity").cast(IntegerType()))
    )

    df_processed = df_clean.withColumn("TotalAmount", col("Unit Price") * col("Quantity"))

    # ---------------------------------------------------------
    # 3. Procesamiento Paralelo y Métricas
    # ---------------------------------------------------------
    sales_by_store = (
        df_processed.filter(col("Transaction Type") == "Sale")
        .groupBy("Store ID")
        .agg(sum("TotalAmount").alias("Ingresos_Totales"))
        .orderBy(desc("Ingresos_Totales"))
    )

    print(">>> Top Ventas por Sucursal:")
    sales_by_store.show(5)

    top_products = (
        df_processed.filter(col("Transaction Type") == "Sale")
        .groupBy("Product ID")
        .agg(sum("Quantity").alias("Unidades_Vendidas"))
        .orderBy(desc("Unidades_Vendidas"))
        .limit(10)
    )

    print(">>> Top 10 Productos:")
    top_products.show()

    total_tx_store = df_processed.groupBy("Store ID").count().withColumnRenamed("count", "Total_Tx")

    failed_tx_store = (
        df_processed.filter(col("Transaction Type") == "Return")
        .groupBy("Store ID")
        .count().withColumnRenamed("count", "Failed_Tx")
    )

    metrics_failure = (
        total_tx_store.join(failed_tx_store, "Store ID", "left_outer")
        .fillna(0)
        .withColumn("Tasa_Fallo", (col("Failed_Tx") / col("Total_Tx")) * 100)
    )

    print(">>> Tasa de Devoluciones por Tienda:")
    metrics_failure.show(5)

    # ---------------------------------------------------------
    # 4. Detección de Anomalías
    # ---------------------------------------------------------
    daily_returns = (
        df_processed.filter(col("Transaction Type") == "Return")
        .groupBy("Date")
        .agg(sum("TotalAmount").alias("Monto_Devuelto"))
    )

    stats = daily_returns.select(
        avg("Monto_Devuelto").alias("media"),
        stddev("Monto_Devuelto").alias("desviacion")
    ).collect()[0]

    umbral = stats["media"] + stats["desviacion"]
    print(f">>> Umbral de Anomalía: {umbral}")

    anomalies = daily_returns.filter(col("Monto_Devuelto") > umbral)

    print(">>> Días con Picos Anómalos de Devoluciones:")
    anomalies.show()

    # ---------------------------------------------------------
    # 5. Persistencia de Resultados
    # ---------------------------------------------------------
    sales_by_store.write.mode("overwrite").parquet("../output/ventas_por_tienda")
    anomalies.coalesce(1).write.mode("overwrite").option("header", "true").csv("../output/anomalias_detectadas")
    print(">>> Resultados guardados exitosamente.")

    # ---------------------------------------------------------
    # Mantener Spark UI viva
    # ---------------------------------------------------------
    #print("\n>>> Manteniendo la sesión activa para revisar Spark UI...")
    #input("Presione Enter para terminar la ejecución y cerrar Spark UI...")
    spark.stop()

if __name__ == "__main__":
    main()
