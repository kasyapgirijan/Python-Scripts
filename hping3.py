import re
import subprocess

def parse_masscan_results(file_path):
    results = []
    with open(file_path, "r") as file:
        for line in file:
            match = re.search(r"Host: ([\d\.]+).*?Ports: (\d+)/open", line)
            if match:
                ip = match.group(1)
                port = match.group(2)
                results.append((ip, port))
    return results

def check_port_status(ip, port):
    try:
        result = subprocess.run([
            "sudo", "hping3", "-S", "-p", port, "-c", "1", ip
        ], capture_output=True, text=True, timeout=5)
        
        if "flags=SA" in result.stdout:
            return "OPEN"
        else:
            return "CLOSED or FILTERED"
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
