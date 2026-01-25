import socket
import json
from pyspark.sql import SparkSession

from utils import *

HOST = "0.0.0.0"
PORT = 5000
DATA_PATH = "./data/transactions.csv"


def start_spark():
    spark = (
        SparkSession.builder \
        .appName("ProcesamientoClothingRetail") \
        .master("local[*]") \
        .config("spark.ui.port", "4040") \
        .config("spark.ui.bindAddress", "0.0.0.0") \
        .config("spark.driver.bindAddress", "0.0.0.0") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.executor.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def process_request(df, request):
    operation = request.get("operation")
    params = request.get("params", {})

    if operation == "top_sales":
        return top_sales(df, params.get("top_n", 5))

    elif operation == "total_sales_by_branch":
        return total_sales_by_branch(df, params.get("top_n", 10))

    elif operation == "top_selling_products":
        return top_selling_products(df)

    elif operation == "return_rate_by_store":
        return return_rate_by_store(df)

    elif operation == "anomalous_return_days":
        return anomalous_return_days(df, params.get("z_threshold", 2.0))    

    else:
        raise ValueError("Operación no válida")


def main():
    spark = start_spark()
    df = load_and_prepare_transactions(spark, DATA_PATH)

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

            output = result_df.show(truncate=False)

            rows = result_df.toJSON().collect()
            conn.sendall(json.dumps(rows).encode())

        except Exception as e:
            conn.sendall(str(e).encode())

        finally:
            conn.close()


if __name__ == "__main__":
    main()
