import socket
import json
import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv

from utils import *

load_dotenv()

HOST = os.getenv("SERVER_HOST")
PORT = int(os.getenv("SERVER_PORT"))

DATA_PATH = "./data/transactions.csv"
stores_df = None
products_df = None

def start_spark():
    spark = (
        SparkSession.builder
        .appName("ProcesamientoClothingRetail")
        .master("spark://10.0.20.1:7077")
        .config("spark.ui.port", "4040")
        .config("spark.ui.bindAddress", "0.0.0.0")
        .config("spark.driver.host", "10.0.20.1")
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.executor.memory", "2g")
        .config("spark.executor.cores", "1")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.jars", "./dependencies/postgresql-42.7.3.jar")

        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    print(">>> Spark iniciado en modo cluster")
    print(">>> Master:", spark.sparkContext.master)
    print(">>> Spark UI:", spark.sparkContext.uiWebUrl)

    return spark


def process_request(df, request):
    operation = request.get("operation")
    params = request.get("params", {})

    if operation == "top_sales":
        log_operation("Top de sucursales con más ventas")
        return top_sales(df, stores_df, params.get("top_n", 5))

    elif operation == "total_sales_by_branch":
        log_operation("Ventas totales por sucursal")
        return total_sales_by_branch(df, stores_df, params.get("id_sucursal", 10))

    elif operation == "top_selling_products":
        log_operation("Top de productos más vendidos")
        return top_selling_products(df, products_df)

    elif operation == "return_rate_by_store":
        log_operation("Tasa de devoluciones por tienda")
        return return_rate_by_store(df, stores_df)

    elif operation == "anomalous_return_days":
        log_operation("Días con devoluciones anómalas")
        return anomalous_return_days(df)    

    elif operation == "top_products_by_store":
        log_operation("Top productos por sucursal")
        return top_selling_products_by_store(
            df,
            products_df, 
            params.get("store_id"),
            params.get("top_n", 10)
        )

    elif operation == "list_stores":
        log_operation("Listado de tiendas")
        return stores_df.select("Store_ID", "Store_Name").orderBy("Store_ID")

    else:
        raise ValueError("Operación no válida")

def log_operation(op_name: str):
    print(f"[SERVER] Operación ejecutada: {op_name}")

def main():
    global stores_df, products_df

    spark = start_spark()
    df = load_and_prepare_transactions(spark, DATA_PATH)

    stores_df = load_sales_from_postgres(spark, "stores") \
    .select("Store_ID", "Store_Name")

    products_df = load_sales_from_postgres(spark, "products") \
    .select("Product_ID", "Description_EN")

    stores_df.cache()
    products_df.cache()

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((HOST, PORT))
    server.listen()

    print(f">>> Servidor escuchando en {HOST}:{PORT}")

    while True:
        conn, addr = server.accept()
        print(f">>> Conexión desde {addr}")

        try:
            data = conn.recv(4096).decode()
            request = json.loads(data)

            result_df = process_request(df, request)

            rows = result_df.toJSON().collect()
            conn.sendall(json.dumps(rows).encode())

        except Exception as e:
            error_response = {
                "error": str(e)
            }
            conn.sendall(json.dumps(error_response).encode())

        finally:
            conn.close()


if __name__ == "__main__":
    main()
