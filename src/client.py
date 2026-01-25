import socket
import json
import os
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
    print("\n" + Colors.CYAN + Colors.BOLD + "=" * 70)
    print(title.center(70))
    print("=" * 70 + Colors.RESET + "\n")

load_dotenv()

HOST = os.getenv("CLIENT_HOST")
PORT = int(os.getenv("CLIENT_PORT"))

def send_request(payload):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((HOST, PORT))
    s.sendall(json.dumps(payload).encode())

    data = s.recv(200000).decode()
    s.close()

    return json.loads(data)

def get_stores():
    payload = {
        "operation": "list_stores"
    }
    result = send_request(payload)
    return [json.loads(r) for r in result]

def select_store():
    stores = get_stores()

    print("\n=== Seleccione una sucursal ===\n")
    for store in stores:
        print(f"[{store['Store_ID']}] {store['Store_Name']}")

    while True:
        try:
            store_id = int(input("\nIngrese el ID de la sucursal: "))
            if any(s["Store_ID"] == store_id for s in stores):
                return store_id
            else:
                print("ID inválido, intente nuevamente.")
        except ValueError:
            print("Ingrese un número válido.")

def input_int(prompt, min_val=None, max_val=None):
    while True:
        try:
            value = int(input(prompt))

            if min_val is not None and value < min_val:
                print(f"El valor debe ser >= {min_val}")
                continue

            if max_val is not None and value > max_val:
                print(f"El valor debe ser <= {max_val}")
                continue

            return value

        except ValueError:
            print("Ingrese un número válido.")

def print_result(result, title=None):
    if title:
        print_title(title)

    if isinstance(result, dict) and "error" in result:
        print(Colors.RED +"\nError:"+ Colors.RESET, result["error"])
        return

    if not result:
        print(Colors.YELLOW +"\nSin resultados"+ Colors.RESET)
        return

    rows = [json.loads(r) for r in result]    
    print(tabulate(rows, headers="keys", tablefmt="grid"))

def main():
    os.system("clear")
    while(True):   
        print(Colors.CYAN + Colors.BOLD + """
            ╔═════════════════════════════════════════════════╗
            ║   APLICACIÓN DE ANÁLISIS DE VENTAS MAYORISTAS   ║
            ╚═════════════════════════════════════════════════╝
        """ + Colors.RESET) 
        print("""
            1. Top de sucursales con más ventas
            2. Ventas totales por sucursal
            3. Top de Productos más vendidos
            4. Productos más vendidos por sucursal
            5. Tasa de devoluciones global
            6. Días con devoluciones anómalas 
            7. Salir
        """)

        option = input_int(Colors.BOLD +"Seleccione una opción: "+ Colors.RESET, 1, 7)

        if option == 1:
            payload = {
                "operation": "top_sales",
                "params": {"top_n": input_int(Colors.BOLD +"Ingrese el valor del tamaño del top: "+ Colors.RESET, 1, 35)}
            }

        elif option == 2:
            payload = {
                "operation": "total_sales_by_branch",
                "params": {"id_sucursal": select_store()}
            }

        elif option == 3:
            payload = {
                "operation": "top_selling_products",
                "params": {"top_n": input_int(Colors.BOLD +"Ingrese el valor del tamaño del top: "+ Colors.RESET, 1, 100)}
            }

        elif option == 4:
            payload = {
                "operation": "top_products_by_store",
                "params": {
                    "store_id": select_store(),
                    "top_n": input_int(Colors.BOLD +"Ingrese el valor del tamaño del top: "+ Colors.RESET, 1, 100)
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
            print("Saliendo del sistema...")    
            break

        else:
            print("Opción inválida")
            return

        print(Colors.BLUE +"\nRealizando petición al servidor...\n" + Colors.RESET)
        try:
            result = send_request(payload)

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

        except Exception as e:
            print(Colors.RED + f"\nError al comunicarse con el servidor: {e}" + Colors.RESET)

        input(Colors.YELLOW + "\n\tPRESIONE UNA TECLA PARA CONTINUAR..." + Colors.RESET)
        os.system("clear")


if __name__ == "__main__":
    main()
