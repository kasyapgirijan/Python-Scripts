import os
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.edge.service import Service
from selenium.webdriver.edge.options import Options

# Define the root directory for all paths
ROOT_DIR = "C:/path/to/your/project"  # Replace with your project's root directory
DOWNLOAD_DIR = os.path.join(ROOT_DIR, "downloads")  # Directory to save downloads
WEBDRIVER_PATH = os.path.join(ROOT_DIR, "drivers", "edgedriver.exe")  # Path to EdgeDriver

# Setup Edge WebDriver with options for SSO
def setup_edge_driver():
    edge_options = Options()
    edge_options.add_argument("--start-maximized")  # Start maximized
    # Uncomment for headless mode
    # edge_options.add_argument("--headless")
    edge_options.add_argument("--disable-gpu")      # Disable GPU acceleration
    
    # Set up download preferences
    prefs = {
        "download.default_directory": DOWNLOAD_DIR,  # Set default download directory
        "download.prompt_for_download": False,        # Disable download prompt
        "download.directory_upgrade": True,           # Auto upgrade if directory exists
        "safebrowsing.enabled": True                  # Allow downloads
    }
    edge_options.add_experimental_option("prefs", prefs)
    
    # Enable SSO
    edge_options.add_argument("--allow-insecure-localhost")
    edge_options.add_argument("--auth-server-whitelist=*")
    edge_options.add_argument("--auth-negotiate-delegate-whitelist=*")

    service = Service(WEBDRIVER_PATH)
    driver = webdriver.Edge(service=service, options=edge_options)
    return driver

# Automate the download process
def download_dashboard_data(driver, url, download_button_selector):
    try:
        # Navigate to the dashboard URL
        driver.get(url)
        time.sleep(5)  # Adjust as needed for the dashboard to load

        # SSO login should be automatic if the system is already authenticated
        print("SSO login should be automatic. Waiting for dashboard to load...")

        # Locate and click the download button
        download_button = driver.find_element(By.CSS_SELECTOR, download_button_selector)
        download_button.click()
        print("Download button clicked. Waiting for the file to download...")

        # Wait for download to complete (adjust as per file size/network speed)
        time.sleep(10)

    except Exception as e:
        print(f"Error during the download process: {e}")
    finally:
        driver.quit()

# Main function
if __name__ == "__main__":
    url = "https://proofpoint-tap-dashboard-url"  # Replace with the actual URL
    download_button_selector = "button.download-button-class"  # Replace with the button's actual CSS selector

    # Ensure the download directory exists
    if not os.path.exists(DOWNLOAD_DIR):
        os.makedirs(DOWNLOAD_DIR)

    # Set up WebDriver and start the download process
    driver = setup_edge_driver()
    download_dashboard_data(driver, url, download_button_selector)
