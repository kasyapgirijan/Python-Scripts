#!/usr/bin/env python3
import ipaddress
import argparse
import sys

def expand_ip_range(range_str):
    try:
        start_str, end_str = [p.strip() for p in range_str.split("-", 1)]
        start = ipaddress.IPv4Address(start_str)
        end = ipaddress.IPv4Address(end_str)
    except Exception as e:
        raise ValueError(f"Invalid range format: '{range_str}'. Use A.B.C.D-W.X.Y.Z") from e

    # allow reversed inputs
    s, e = (int(start), int(end)) if int(start) <= int(end) else (int(end), int(start))

    for n in range(s, e + 1):
        yield str(ipaddress.IPv4Address(n))

def expand_multiple_ranges(ranges_str):
    """Expand multiple ranges separated by comma or space"""
    all_ips = []
    for part in ranges_str.replace(" ", "").split(","):
        if not part:
            continue
        all_ips.extend(expand_ip_range(part))
    return all_ips

def main():
    ap = argparse.ArgumentParser(
        description="Expand IPv4 ranges (supports multiple, comma-separated ranges)."
    )
    ap.add_argument(
        "ranges",
        help="IP ranges like '213.193.46.128-213.193.46.191,213.193.47.200-213.193.47.220'"
    )
    ap.add_argument(
        "-o", "--out",
        help="Write output to this file instead of stdout"
    )
    args = ap.parse_args()

    try:
        ips = expand_multiple_ranges(args.ranges)
    except ValueError as ve:
        print(f"Error: {ve}", file=sys.stderr)
        sys.exit(1)

    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            f.write("\n".join(ips) + "\n")
    else:
        print("\n".join(ips))

if __name__ == "__main__":
    main()
