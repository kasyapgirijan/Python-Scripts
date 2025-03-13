import re
import subprocess

def parse_masscan_results(file_path):
    """Extract IP and port details from Masscan output"""
    results = []
    with open(file_path, "r") as file:
        for line in file:
            match = re.search(r"Host: ([\d\.]+).*?Ports: (\d+)/open/tcp", line)
            if match:
                ip = match.group(1)
                port = match.group(2)
                results.append((ip, port))
    return results

def check_port_status(ip, port):
    """Check port status using hping3 and analyze responses"""
    try:
        result = subprocess.run(
            ["sudo", "hping3", "-S", ip, "-p", port, "-c", "5"],
            capture_output=True, text=True, timeout=10
        )

        # Count occurrences of SA (SYN-ACK) and RA (RST-ACK)
        sa_count = result.stdout.count("flags=SA")
        ra_count = result.stdout.count("flags=RA")

        if sa_count > 2:  # Majority SYN-ACK responses mean port is open
            return "OPEN"
        elif ra_count > 2:  # Majority RST-ACK responses mean port is closed
            return "CLOSED"
        else:
            return "FILTERED or NO RESPONSE"
    
    except subprocess.TimeoutExpired:
        return "TIMEOUT"

def main():
    input_file = "masscan_results.txt"
    output_file = "hping_results.txt"

    results = parse_masscan_results(input_file)

    with open(output_file, "w") as out:
        for ip, port in results:
            print(f"Checking port {port} on {ip}...")
            status = check_port_status(ip, port)
            result_line = f"Port {port} on {ip} is {status}\n"
            print(result_line.strip())
            out.write(result_line)
    
    print(f"Results saved to {output_file}")

if __name__ == "__main__":
    main()
