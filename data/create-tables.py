import pandas as pd
from sqlalchemy import create_engine
import os

DB_HOST = "10.0.20.20"
DB_PORT = 5432
DB_NAME = "retaildb"
DB_USER = "postgres"
DB_PASSWORD = "postgres"

DATA_DIR = "./data"

def load_csv(table_name, filename):
    path = os.path.join(DATA_DIR, filename)
    print(f"\nCargando {path} -> tabla {table_name}")

    df = pd.read_csv(path)

    df.columns = [c.strip().replace(" ", "_") for c in df.columns]

    print("Columnas:", df.columns.tolist())
    print("Filas:", len(df))

    df.to_sql(table_name, engine, if_exists="replace", index=False)
    print("✅ Tabla cargada:", table_name)

if __name__ == "__main__":
    engine = create_engine(
        f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )

    load_csv("stores", "stores.csv")
    load_csv("products", "products.csv")
    load_csv("transactions", "transactions.csv")

    print("\n✅ Carga completa")
