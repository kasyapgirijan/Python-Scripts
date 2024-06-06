"""
Usage Instructions:
------------------

1. Ensure you have Python installed on your system.

2. Place this script in the directory where your photos/videos are stored or in the parent directory.

3. Run the script by executing it with Python:
   $ python script_name.py

4. The script will organize the files based on their creation dates into subdirectories within the same folder.

5. Once the script completes execution, check the directory structure for the organized files.

6. You can customize the script by modifying the file extensions it processes or the destination folder structure.

Note: This script modifies the directory structure by rearranging files. Make sure to review the changes before running the script in a critical environment.
"""

import os
import shutil
from PIL import Image
import exifread
import pyheif
from datetime import datetime

# Function to get the creation date from an image or video file
def get_creation_date(file_path):
    try:
        # Try to extract creation date from EXIF data for JPEG files
        if file_path.lower().endswith('.jpg') or file_path.lower().endswith('.jpeg'):
            # Use PIL to extract EXIF data
            with Image.open(file_path) as img:
                exif_data = img._getexif()
                if exif_data and 36867 in exif_data:
                    date_str = exif_data[36867]
                    date_obj = datetime.strptime(date_str, '%Y:%m:%d %H:%M:%S')
                    return date_obj

        # Try to extract creation date from other image formats, HEIC, video files, etc.
        with open(file_path, 'rb') as file:
            tags = exifread.process_file(file, stop_tag='EXIF DateTimeOriginal')
            if 'EXIF DateTimeOriginal' in tags:
                date_str = str(tags['EXIF DateTimeOriginal'])
                date_obj = datetime.strptime(date_str, '%Y:%m:%d %H:%M:%S')
                return date_obj

        # If EXIF data doesn't contain date, try to extract it from HEIC metadata
        if file_path.lower().endswith('.heic'):
            try:
                heif_file = pyheif.read(file_path)
                creation_date = heif_file.metadata['Exif']['DateTimeOriginal'].decode('utf-8')
                date_obj = datetime.strptime(creation_date, '%Y:%m:%d %H:%M:%S')
                return date_obj
            except (pyheif.NotFoundError, KeyError):
                print(f"No creation date found in HEIC metadata for {file_path}")

        # Fallback to file modification time
        modification_time = os.path.getmtime(file_path)
        return datetime.fromtimestamp(modification_time)

    except Exception as e:
        print(f"Error reading date from {file_path}: {e}")
    return None

# Function to move a file to the destination folder within the same directory structure
def move_file(source_path, dest_folder):
    if not os.path.exists(dest_folder):
        os.makedirs(dest_folder)
    shutil.move(source_path, os.path.join(dest_folder, os.path.basename(source_path)))

# Function to delete empty folders in a directory
def delete_empty_folders(source_folder):
    for root, dirs, _ in os.walk(source_folder, topdown=False):
        for dir_name in dirs:
            folder_path = os.path.join(root, dir_name)
            if not os.listdir(folder_path): # Check if the folder is empty
                try:
                    os.rmdir(folder_path)
                    print(f"Deleted empty folder: {folder_path}")
                except Exception as e:
                    print(f"Error deleting folder {folder_path}: {e}")

# Function to rearrange files in the source folder and its subfolders
def rearrange_files(source_folder):
    for root, _, files in os.walk(source_folder):
        for file_name in files:
            file_path = os.path.join(root, file_name)
            creation_date = get_creation_date(file_path)
            if creation_date:
                print(f"File: {file_path} | Creation Date: {creation_date}")
                year = str(creation_date.year)
                month = creation_date.strftime('%m')  # Use '%m' for MM format
                dest_folder = os.path.join(source_folder, year, month)
                move_file(file_path, dest_folder)
            else:
                print(f"Skipping {file_path} - Unable to determine creation date.")

if __name__ == "__main__":
    # Get the directory where the script is located
    script_dir = os.path.dirname(os.path.realpath(__file__))
    source_folder = script_dir
    rearrange_files(source_folder)
    delete_empty_folders(source_folder)
