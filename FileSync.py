"""
Usage:
------
This script copies all files from a source directory to a destination directory.

Command-line arguments:
  source_dir          Path to the source directory.
  destination_dir     Path to the destination directory.
  -v, --verbose       Enable verbose mode.

Example usage:
--------------
To copy files from '/path/to/source' to '/path/to/destination' with verbose output:
    python script_name.py /path/to/source /path/to/destination -v

To copy files without verbose output:
    python script_name.py /path/to/source /path/to/destination
"""

import os
import shutil
import argparse
from tqdm import tqdm
import time
import math

def convert_size(size_bytes):
    if size_bytes == 0:
        return "0B"
    size_name = ("B", "KB", "MB", "GB", "TB")
    i = int(math.log(size_bytes, 1024))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return f"{s} {size_name[i]}"

def copy_all_files(source_dir, destination_dir, verbose=False):

    if not os.path.exists(destination_dir):
        os.makedirs(destination_dir)

    files_to_copy = [f for f in os.listdir(source_dir) if os.path.isfile(os.path.join(source_dir, f))]
    total_files = len(files_to_copy)
    copied_files_count = 0
    failed_files = []

    if verbose:
        print(f"Total files to copy: {total_files}")

    start_time = time.time()

    with tqdm(total=total_files, unit='file') as pbar:
        for filename in files_to_copy:
            source_path = os.path.join(source_dir, filename)
            destination_path = os.path.join(destination_dir, filename)
            file_size = os.path.getsize(source_path)
            file_size_readable = convert_size(file_size)
            try:
                shutil.copy2(source_path, destination_path)
                copied_files_count += 1
                if verbose:
                    elapsed_time = time.time() - start_time
                    eta = elapsed_time / copied_files_count * (total_files - copied_files_count)
                    pbar.set_description(f"Copied: {filename} (Size: {file_size_readable}) ETA: {int(eta)}s")
                pbar.update(1)
            except Exception as e:
                failed_files.append((filename, str(e)))
                if verbose:
                    pbar.set_description(f"Failed to copy: {filename} due to {str(e)}")
                pbar.update(1)

    print(f"Successfully copied files: {copied_files_count} out of {total_files}")
    if failed_files:
        print("Failed to copy the following files:")
        for filename, error in failed_files:
            print(f"{filename}: {error}")
    else:
        print("All files copied successfully.")

def dry_run(source_dir):
    """
    Performs a dry run to count the number of files to be copied.

    Args:
        source_dir: Path to the source directory.

    Returns:
        total_files: The total number of files to be copied.
    """
    files_to_copy = [f for f in os.listdir(source_dir) if os.path.isfile(os.path.join(source_dir, f))]
    total_files = len(files_to_copy)
    return total_files

# Set up command line argument parsing
parser = argparse.ArgumentParser(description="Copy all files from source directory to destination directory.")
parser.add_argument('source_dir', help="Path to the source directory")
parser.add_argument('destination_dir', help="Path to the destination directory")
parser.add_argument('-v', '--verbose', action='store_true', help="Enable verbose mode")

args = parser.parse_args()

# Perform a dry run to count files
total_files = dry_run(args.source_dir)
print(f"Total files to copy: {total_files}")

# Ask for user confirmation
confirmation = input("Do you want to proceed with copying the files? (yes/no): ").strip().lower()
if confirmation == 'yes':
    copy_all_files(args.source_dir, args.destination_dir, args.verbose)
else:
    print("Operation cancelled.")

