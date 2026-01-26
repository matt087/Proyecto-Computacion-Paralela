import socket
import json
import os
import time
from tabulate import tabulate
from dotenv import load_dotenv

class Colors:
    BLUE = "\033[94m"
    CYAN = "\033[96m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    BOLD = "\033[1m"
    RESET = "\033[0m"

def print_title(title):
    print("\n" + Colors.CYAN + Colors.BOLD + "=" * 80)
    print(title.center(80))
    print("=" * 80 + Colors.RESET + "\n")

load_dotenv()

HOST = os.getenv("CLIENT_HOST")
PORT = int(os.getenv("CLIENT_PORT"))

def send_request(payload):
    start_time = time.perf_counter()

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((HOST, PORT))
    s.sendall(json.dumps(payload).encode())
    s.shutdown(socket.SHUT_WR)
    
    chunks = []
    while True:
        part = s.recv(4096)
        if not part:
            break
        chunks.append(part)

    s.close()
    data = b"".join(chunks).decode()

    end_time = time.perf_counter()
    elapsed = end_time - start_time

    return json.loads(data), elapsed


def get_stores():
    payload = {
        "operation": "list_stores"
    }
    result = send_request(payload)
    if isinstance(result, dict) and "error" in result:
        print("Error:", result["error"])
        return []
    return result

def select_store():
    os.system("clear")
    stores = get_stores()

    print(Colors.CYAN + "\n=== Seleccione una sucursal ===\n" + Colors.RESET)
    for store in stores:
        print(f"{Colors.GREEN}[{store['Store_ID']}]{Colors.RESET} {store['Store_Name']}")

    while True:
        try:
            store_id = int(input(Colors.YELLOW + "\nIngrese el ID de la sucursal: " + Colors.RESET))
            if any(s["Store_ID"] == store_id for s in stores):
                return store_id
            else:
                print(Colors.RED + "ID inválido, intente nuevamente." + Colors.RESET)
        except ValueError:
            print(Colors.RED + "Ingrese un número válido." + Colors.RESET)

def input_int(prompt, min_val=None, max_val=None):
    while True:
        try:
            value = int(input(prompt))

            if min_val is not None and value < min_val:
                print(Colors.RED + f"El valor debe ser >= {min_val}" + Colors.RESET)
                continue

            if max_val is not None and value > max_val:
                print(Colors.RED + f"El valor debe ser <= {max_val}" + Colors.RESET)
                continue

            return value

        except ValueError:
            print(Colors.RED + "Ingrese un número válido." + Colors.RESET)

def print_result(result, title=None):
    if title:
        print_title(title)

    if isinstance(result, dict) and "error" in result:
        print(Colors.RED + "\nError:" + Colors.RESET, result["error"])
        return

    if not result:
        print(Colors.YELLOW + "\nSin resultados" + Colors.RESET)
        return

    print(tabulate(result, headers="keys", tablefmt="grid"))

def main():
    os.system("clear")
    while(True):    
        print(Colors.CYAN + Colors.BOLD + """
            ╔═════════════════════════════════════════════════╗
            ║    APLICACIÓN DE ANÁLISIS DE VENTAS MAYORISTAS  ║
            ╚═════════════════════════════════════════════════╝
        """ + Colors.RESET) 
        print(f"""
            {Colors.GREEN}1.{Colors.RESET} Top de sucursales con más ventas
            {Colors.GREEN}2.{Colors.RESET} Ventas totales por sucursal
            {Colors.GREEN}3.{Colors.RESET} Top de Productos más vendidos
            {Colors.GREEN}4.{Colors.RESET} Productos más vendidos por sucursal
            {Colors.GREEN}5.{Colors.RESET} Tasa de devoluciones global
            {Colors.GREEN}6.{Colors.RESET} Días con devoluciones anómalas 
            {Colors.GREEN}7.{Colors.RESET} Salir
        """)

        option = input_int(Colors.YELLOW + Colors.BOLD + "Seleccione una opción: " + Colors.RESET, 1, 7)

        if option == 1:
            payload = {
                "operation": "top_sales",
                "params": {"top_n": input_int(Colors.YELLOW + "\nIngrese el valor del tamaño del top: " + Colors.RESET, 1, 35)}
            }

        elif option == 2:
            payload = {
                "operation": "total_sales_by_branch",
                "params": {"id_sucursal": select_store()}
            }

        elif option == 3:
            payload = {
                "operation": "top_selling_products",
                "params": {"top_n": input_int(Colors.YELLOW + "\nIngrese el valor del tamaño del top: " + Colors.RESET, 1, 100)}
            }

        elif option == 4:
            payload = {
                "operation": "top_products_by_store",
                "params": {
                    "store_id": select_store(),
                    "top_n": input_int(Colors.YELLOW + "\nIngrese el valor del tamaño del top: " + Colors.RESET, 1, 100)
                }
            }
        elif option == 5:
            payload = {
                "operation": "return_rate_by_store",
            }
        elif option == 6:
            payload = {
                "operation": "anomalous_return_days"
            }     
        
        elif option == 7:
            print(Colors.BLUE + "\nSaliendo del sistema..." + Colors.RESET)     
            break

        else:
            print(Colors.RED + "Opción inválida" + Colors.RESET)
            return

        print(Colors.BLUE + "\nRealizando petición al servidor...\n" + Colors.RESET)
        try:
            result, elapsed = send_request(payload)

            titles = {
                1: "Top de Sucursales con Más Ventas",
                2: "Ventas Totales por Sucursal",
                3: "Top de Productos Más Vendidos",
                4: "Productos Más Vendidos por Sucursal",
                5: "Tasa de Devoluciones Global",
                6: "Días con Devoluciones Anómalas"
            }
            os.system("clear")
            print_result(result, titles.get(option))
            print(f"{Colors.GREEN}Tiempo de ejecución:{Colors.RESET} {elapsed:.3f} segundos")


        except Exception as e:
            print(Colors.RED + f"\nError al comunicarse con el servidor: {e}" + Colors.RESET)

        input(Colors.YELLOW + "\n\tPRESIONE UNA TECLA PARA CONTINUAR..." + Colors.RESET)
        os.system("clear")


if __name__ == "__main__":
    main()