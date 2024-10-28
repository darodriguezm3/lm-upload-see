import requests
import sys
from datetime import datetime
from dotenv import load_dotenv
import os
import json
import logging
from collections import Counter

# Load environment variables
load_dotenv()
logging.basicConfig(level=logging.INFO)

LUMU_CLIENT_KEY = os.getenv('LUMU_CLIENT_KEY')
COLLECTOR_ID = os.getenv('COLLECTOR_ID')
API_URL = os.getenv('API_URL')

def validate_environment_variables():
    """
    Validates that all required environment variables are set.
    """
    required_vars = ['LUMU_CLIENT_KEY', 'COLLECTOR_ID', 'API_URL']
    missing_vars = [var for var in required_vars if not globals().get(var)]
    if missing_vars:
        logging.error(f"Missing environment variables: {', '.join(missing_vars)}")
        sys.exit(1)

def convert_date(date_str):
    """
    Changes date format from "7-Jul-2022" to "2022-07-07".
    """
    try:
        date_obj = datetime.strptime(date_str, "%d-%b-%Y")
        return date_obj.strftime("%Y-%m-%d")
    except ValueError:
        logging.error(f"Incorrect date format: {date_str}")
        return None

def parse_bind_log(file_path):
    """
    Parses the BIND log file and extracts client IPs and hostnames.
    """
    with open(file_path, 'r') as f:
        for line in f:
            try:
                element_list = line.strip().split()
                if len(element_list) < 12:
                    continue  # Skip lines with incorrect format

                date_str = element_list[0]
                time_str = element_list[1]
                timestamp = convert_date(date_str)
                if not timestamp:
                    continue
                timestamp += 'T' + time_str + 'Z'
                client_ip = element_list[6].split('#')[0]
                client_name = element_list[5].replace('@', '')
                question_type = element_list[11]
                name = element_list[9]

                yield {
                    'timestamp': timestamp,
                    'name': name,
                    'client_ip': client_ip,
                    'client_name': client_name,
                    'type': question_type
                }
            except Exception as e:
                logging.error(f"Error parsing line: {e}")
                continue

def send_data_to_lumu(data_chunk):
    """
    Sends a data chunk to the Lumu API.
    """
    headers = {
        'Content-Type': 'application/json'
    }

    final_URL = f"{API_URL}/collectors/{COLLECTOR_ID}/dns/queries?key={LUMU_CLIENT_KEY}"
    
    try:
        response = requests.post(final_URL, json=data_chunk, headers=headers)
        response.raise_for_status()
        logging.info("Data sent successfully.")
    except requests.exceptions.RequestException as e:
        logging.error(f"Error sending data: {e}")

def calculate_statistics(total_records, client_ip_counts, name_counts):
    """
    Computes and displays statistics from client IPs and names.
    """
    top_client_ips = client_ip_counts.most_common(5)
    top_names = name_counts.most_common(5)
    
    print(f"Total records: {total_records}\n")
    
    # Display client IP ranking
    print("Client IPs Ranking")
    print("------------------")
    for ip, count in top_client_ips:
        print(f"{ip} {count} {count / total_records * 100:.2f}%")
    
    # Display host ranking
    print("\nHost Ranking")
    print("------------")
    for host, count in top_names:
        print(f"{host} {count} {count / total_records * 100:.2f}%")

def main():
    validate_environment_variables()
    
    if len(sys.argv) != 2:
        print("Usage: python dns_collector.py <log_file>")
        sys.exit(1)
    
    log_file = sys.argv[1]
    
    CHUNK_SIZE = 500
    data_chunk = []
    total_records = 0
    client_ip_counts = Counter()
    name_counts = Counter()
    
    for entry in parse_bind_log(log_file):
        data_chunk.append(entry)
        total_records += 1

        client_ip_counts.update([entry['client_ip']])
        name_counts.update([entry['name']])

        if len(data_chunk) >= CHUNK_SIZE:
            send_data_to_lumu(data_chunk)
            data_chunk = []
    
    # Send any remaining data
    if data_chunk:
        send_data_to_lumu(data_chunk)

    # Calculate and display statistics
    calculate_statistics(total_records, client_ip_counts, name_counts)

if __name__ == "__main__":
    main()
