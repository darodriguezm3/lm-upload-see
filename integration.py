import requests
import sys
from datetime import datetime
import os


LUMU_CLIENT_KEY = os.environ.get('LUMU_CLIENT_KEY')
COLLECTOR_ID = os.environ.get('COLLECTOR_ID')
API_URL = os.environ.get('API_URL')

def convert_date(date_str):
    # Convierte la fecha del formato "7-Jul-2022" al formato "2022-07-07"
    date_obj = datetime.strptime(date_str, "%d-%b-%Y")
    return date_obj.strftime("%Y-%m-%d")



def parse_bind_log(file_path):
    """
    Analiza el archivo de log BIND y extrae las IPs de clientes y hosts.
    """
    request_json = []
    
    with open(file_path, 'r') as f:
        for line in f:
            # Ejemplo de línea de log: 
            # "18-May-2021 16:34:13.003 queries: info: client @0x55adcc672cc0 45.231.61.2#80 (pizzaseo.com): query: pizzaseo.com IN A +E(0)D (172.20.101.44)"
            element_list = line.split()
            timestamp = convert_date(element_list[0]) + 'T' + element_list[1] + 'Z'
            client_ip = element_list[6].split('#')[0]
            client_name = element_list[5].replace('@', '')
            question_type = element_list[11]
            name = element_list[9]
            
            request_json.append({
                'timestamp': timestamp,
                'name': name,
                'client_ip': client_ip,
                'client_name': client_name,
                'type': question_type
            })
    
    return request_json

def send_data_to_lumu(data_chunk):
    """
    Envía un chunk de datos al API de Lumu.
    """
    headers = {
        'Authorization': f'Lumu {LUMU_CLIENT_KEY}',
        'Content-Type': 'application/json'
    }
    payload = {
        'collector-id': COLLECTOR_ID,
        'data': data_chunk
    }
    
    response = requests.post(API_URL, json=payload, headers=headers)
    if response.status_code == 200:
        print("Datos enviados correctamente.")
    else:
        print(f"Error al enviar datos: {response.status_code}")



def calculate_statistics(data):
    """
    Calcula y muestra estadísticas de IPs de clientes y nombres de hosts
    a partir de una lista de diccionarios con las claves `client_ip` y `name`.
    """
    total_records = len(data)
    
    # Extrae los valores de client_ip y name de cada diccionario
    client_ips = [entry['client_ip'] for entry in data]
    name = [entry['name'] for entry in data]


    def count_occurrences(items):
        counts = []
        unique_items = []
        
        for item in items:
            if item not in unique_items:
                unique_items.append(item)
                count = 0
                for i in items:
                    if i == item:
                        count += 1
                counts.append((item, count))
        
        return counts
    
    # Calcula el ranking de IPs y hosts
    client_ip_counts = count_occurrences(client_ips)
    client_name_counts = count_occurrences(name)
    
    # Ordena los resultados por el conteo en orden descendente
    client_ip_counts.sort(key=lambda x: x[1], reverse=True)
    client_name_counts.sort(key=lambda x: x[1], reverse=True)
    
    # Muestra el total de registros
    print(f"Total records: {total_records}\n")
    
    # Muestra el ranking de IPs
    print("Client IPs Rank")
    print("---------------")
    for ip, count in client_ip_counts[:5]:
        print(f"{ip} {count} {count / total_records * 100:.2f}%")
    
    # Muestra el ranking de hosts
    print("\nHost Rank")
    print("---------------")
    for host, count in client_name_counts[:5]:
        print(f"{host} {count} {count / total_records * 100:.2f}%")


def main():
    # Verifica si se pasa el archivo como argumento
    if len(sys.argv) != 2:
        print("Uso: python dns_collector.py <archivo_log>")
        sys.exit(1)
    
    log_file = sys.argv[1]
    
    # Procesa el archivo de log
    json_body = parse_bind_log(log_file)
    
    # Envía los datos en chunks de 500 registros
    CHUNK_SIZE = 500
    for i in range(0, len(json_body), CHUNK_SIZE):
        data_chunk = json_body[i:i + CHUNK_SIZE]
        print(len(data_chunk))
        #send_data_to_lumu(data_chunk)
    
    # Calcula y muestra las estadísticas
    calculate_statistics(json_body)

if __name__ == "__main__":
    main()
