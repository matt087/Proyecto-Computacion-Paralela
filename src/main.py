#Librerías utilizadas
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, desc, when, avg, stddev
from pyspark.sql.types import DoubleType, IntegerType

def main():
    # ---------------------------------------------------------
    # 1. Configuración del Entorno Spark 
    # ---------------------------------------------------------
    spark = SparkSession.builder \
        .appName("ProcesamientoClothingRetail") \
        .master("local[*]") \
        .config("spark.executor.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN") 

    print(">>> Entorno Spark Iniciado")

    # ---------------------------------------------------------
    # 2. Carga y Preparación de Datos
    # ---------------------------------------------------------
    path = "../data/transactions.csv"
    
    # Lectura de datos
    df_raw = spark.read.csv(path, header=True, inferSchema=True)
    print(f">>> Registros cargados: {df_raw.count()}")

    # Limpieza: Filtrar nulos en columnas críticas y asegurar tipos
    df_clean = df_raw.filter(col("Invoice ID").isNotNull() & col("Customer ID").isNotNull()) \
                     .withColumn("Unit Price", col("Unit Price").cast(DoubleType())) \
                     .withColumn("Quantity", col("Quantity").cast(IntegerType()))

    # Creación de columna de Monto Total (Venta)
    df_processed = df_clean.withColumn("TotalAmount", col("Unit Price") * col("Quantity"))

    # ---------------------------------------------------------
    # 3. Procesamiento Paralelo y Métricas 
    # ---------------------------------------------------------
    
    # A. Ventas Totales por Sucursal (Store ID)
    # Transformación: groupBy + agg
    sales_by_store = df_processed.filter(col("Transaction Type") == "Sale") \
                                 .groupBy("Store ID") \
                                 .agg(sum("TotalAmount").alias("Ingresos_Totales")) \
                                 .orderBy(desc("Ingresos_Totales"))
    
    print(">>> Top Ventas por Sucursal:")
    sales_by_store.show(5)

    # B. Top 10 Productos más Vendidos
    top_products = df_processed.filter(col("Transaction Type") == "Sale") \
                               .groupBy("Product ID") \
                               .agg(sum("Quantity").alias("Unidades_Vendidas")) \
                               .orderBy(desc("Unidades_Vendidas")) \
                               .limit(10)
    
    print(">>> Top 10 Productos:")
    top_products.show()

    # C. Porcentaje de Transacciones 'Fallidas' (Devoluciones) por Sucursal
    total_tx_store = df_processed.groupBy("Store ID").count().withColumnRenamed("count", "Total_Tx")
    
    failed_tx_store = df_processed.filter(col("Transaction Type") == "Return") \
                                  .groupBy("Store ID") \
                                  .count().withColumnRenamed("count", "Failed_Tx")

    metrics_failure = total_tx_store.join(failed_tx_store, "Store ID", "left_outer") \
                                    .fillna(0) \
                                    .withColumn("Tasa_Fallo", (col("Failed_Tx") / col("Total_Tx")) * 100)
    
    print(">>> Tasa de Devoluciones por Tienda:")
    metrics_failure.show(5)

    # ---------------------------------------------------------
    # 4. Detección de Anomalías
    # ---------------------------------------------------------
    # Identificar días con devoluciones inusualmente altas (Media + Desviación Estándar)
    # Agrupar devoluciones por fecha
    daily_returns = df_processed.filter(col("Transaction Type") == "Return") \
                                .groupBy("Date") \
                                .agg(sum("TotalAmount").alias("Monto_Devuelto"))

    # Calcular estadísticas globales
    stats = daily_returns.select(
        avg("Monto_Devuelto").alias("media"), 
        stddev("Monto_Devuelto").alias("desviacion")
    ).collect()[0]

    umbral = stats["media"] + stats["desviacion"]
    print(f">>> Umbral de Anomalía: {umbral}")

    # Filtrar días que superan el umbral
    anomalies = daily_returns.filter(col("Monto_Devuelto") > umbral)
    
    print(">>> Días con Picos Anómalos de Devoluciones:")
    anomalies.show()

    # ---------------------------------------------------------
    # 5. Persistencia de Resultados
    # ---------------------------------------------------------
    # Guardamos resultados en archivos Parquet y CSV
    
    sales_by_store.write.mode("overwrite").parquet("../output/ventas_por_tienda")
    anomalies.coalesce(1).write.mode("overwrite").option("header", "true").csv("../output/anomalias_detectadas")
    print(">>> Resultados guardados exitosamente.")
    
    # Mantener sesión activa para ver Spark UI 
    input("Presione Enter para terminar la ejecución y cerrar Spark UI...")
    spark.stop()

if __name__ == "__main__":
    main()