import requests
import sys
from datetime import datetime
from dotenv import load_dotenv
import os
import json

load_dotenv()


LUMU_CLIENT_KEY = os.getenv('LUMU_CLIENT_KEY')
COLLECTOR_ID = os.getenv('COLLECTOR_ID')
API_URL = os.getenv('API_URL')


def convert_date(date_str):
    """"
    Change date formats from "7-Jul-2022" to "2022-07-07"
    """
    date_obj = datetime.strptime(date_str, "%d-%b-%Y")
    return date_obj.strftime("%Y-%m-%d")



def parse_bind_log(file_path):
    """
    Analize the BIND log file and extract the IPs of clients and hosts.
    """
    request_json = []
    
    with open(file_path, 'r') as f:
        for line in f:
            # Line log sample: 
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
    Sends a data chunk to the Lumu API.
    """
    headers = {
        'Content-Type': 'application/json'
    }

    final_URL = API_URL + '/collectors/' + COLLECTOR_ID + '/dns/queries?key=' + LUMU_CLIENT_KEY
    
    response = requests.post(final_URL, json=data_chunk, headers=headers)
    if response.status_code == 200:
        print("Data sent successfully.")
    else:
        print(f"Error sending data: {response.status_code}")
        print(response.text)



def calculate_statistics(data):
    """
    Compute and display statistics from client IPs and names.

    """
    total_records = len(data)
    
    # Extracts values for the dicts
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
    
    #Computes IP and ranking
    client_ip_counts = count_occurrences(client_ips)
    client_name_counts = count_occurrences(name)
    
    # Order results descending
    client_ip_counts.sort(key=lambda x: x[1], reverse=True)
    client_name_counts.sort(key=lambda x: x[1], reverse=True)
    
    # Show results
    print(f"Total records: {total_records}\n")
    
    # Show IP ranking
    print("Client IPs Rank")
    print("---------------")
    for ip, count in client_ip_counts[:5]:
        print(f"{ip} {count} {count / total_records * 100:.2f}%")
    
    # Show Host ranking
    print("\nHost Rank")
    print("---------------")
    for host, count in client_name_counts[:5]:
        print(f"{host} {count} {count / total_records * 100:.2f}%")


def main():
    # Verify if args have been passed
    if len(sys.argv) != 2:
        print("Uso: python dns_collector.py <archivo_log>")
        sys.exit(1)
    
    log_file = sys.argv[1]
    
    # Process the BIND log
    json_body = parse_bind_log(log_file)
    
    # Sends the data in chunks
    CHUNK_SIZE = 10
    for i in range(0, len(json_body), CHUNK_SIZE):
        data_chunk = json_body[i:i + CHUNK_SIZE]
        print(data_chunk)
        send_data_to_lumu(data_chunk)
        input()
    
    # Compute statistics
    calculate_statistics(json_body)

if __name__ == "__main__":
    main()
