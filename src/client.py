import socket
import json
import os
from tabulate import tabulate

HOST = "127.0.0.1"
PORT = 5000

def send_request(payload):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((HOST, PORT))
    s.sendall(json.dumps(payload).encode())

    data = s.recv(200000).decode()
    s.close()

    return json.loads(data)

def print_result(result):
    if isinstance(result, dict) and "error" in result:
        print("\nError:", result["error"])
        return

    if not result:
        print("\nSin resultados")
        return

    rows = [json.loads(r) for r in result]    
    print(tabulate(rows, headers="keys", tablefmt="grid"))

def main():
    os.system("clear")
    while(True):    
        print("""
            \tAplicación de Análisis de Ventas Mayoristas\n
            1. Top de sucursales con más ventas
            2. Ventas totales por sucursal
            3. Top de Productos más vendidos
            4. Tasa de devoluciones por tienda
            5. Días con devoluciones anómalas
            6. Salir
        """)

        option = input("Seleccione una opción: ")

        if option == "1":
            payload = {
                "operation": "top_sales",
                "params": {"top_n": int(input("Top N: "))}
            }

        elif option == "2":
            payload = {
                "operation": "total_sales_by_branch",
                "params": {"top_n": int(input("ID de la sucursal: "))}
            }

        elif option == "3":
            payload = {
                "operation": "top_selling_products"
            }

        elif option == "4":
            payload = {
                "operation": "return_rate_by_store"
            }

        elif option == "5":
            payload = {
                "operation": "anomalous_return_days"
            }    

        elif option == "6":
            print("Saliendo del sistema...")    
            break

        else:
            print("Opción inválida")
            return

        print("\nRealizando petición al servidor...\n")
        result = send_request(payload)
        print_result(result)
        wait_input = input("\n\tPRESIONE UNA TECLA PARA CONTINUAR...")
        os.system("clear")


if __name__ == "__main__":
    main()
