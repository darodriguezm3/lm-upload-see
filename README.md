# DNS Queries Collector

This script is designed to parse a DNS log file from a BIND server, extract key information (client IPs and queried hosts), send the parsed data to the Lumu API in chunks, and print statistics about the DNS queries.

## Requirements
Just install python 3.9 or superior 
It is recommendable to create a virtual environment. There, run the command

pip install -r requieremnts.txt

# Run

To run the script, use the following command:

python dns_collector.py <log_file>
Replace <log_file> with the path to your DNS log file.

Example

python dns_collector.py queries

This command will parse queries, send the DNS query data to the Lumu API in chunks of up to 500 records, and display query statistics in the terminal.

# Output
The script will print the following statistics in the terminal:

Total records: Total number of DNS queries processed.
Client IPs Rank: The top 5 client IPs that made the most queries, along with their counts and percentages.
Host Rank: The top 5 most queried hosts, along with their counts and percentages.
Example Output


Total records: 6000

Client IPs Rank
---------------
45.238.196.2     426  7.10%
179.0.4.14       324  5.40%
76.237.82.109    305  5.08%
87.190.45.164    261  4.35%
186.117.159.151  183  3.05%

Host Rank
---------------
TTRTX.local                                                 305  5.08%
outlook.office.com                                           29  0.48%
global.asimov.events.data.trafficmanager.net                 26  0.43%
MNZ-efz.ms-acdc.office.com                                   24  0.40%
skypedataprdcolcus05.cloudapp.net                            21  0.35%

# Code Structure

parse_bind_log: Parses the DNS log file to extract client IPs and hostnames.
send_data_to_lumu: Sends parsed data to the Lumu API in chunks of up to 500 records.
calculate_statistics: Calculates and displays statistics for client IPs and hostnames.

# Complexity

The ranking function in calculate_statistics uses a counting method with a computational complexity of 𝑂(𝑛2).

Notes
Lumu API Authorization: The script requires a .env file with a Lumu Client Key, the Collector ID to authenticate API requests and the URL to the endpoint.
