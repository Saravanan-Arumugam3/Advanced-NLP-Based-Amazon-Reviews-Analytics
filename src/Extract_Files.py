from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import os
import time

current_directory = os.getcwd()
download_directory = current_directory

# Set up Chrome options to set the download path
chrome_options = Options()
prefs = {
    "download.default_directory": download_directory,
    "download.prompt_for_download": False,  # To automatically save files to the specified directory without asking
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True  # To prevent the 'This type of file can harm your computer' warning
}
chrome_options.add_experimental_option("prefs", prefs)

# Set up the Chrome WebDriver
service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=chrome_options)

# Navigate to the web page
driver.get('https://cseweb.ucsd.edu/~jmcauley/datasets/amazon_v2/#sample-metadata')

# Wait for the page to load
time.sleep(5)  # Replace with a more robust wait strategy in production code

# Find all "5-core" links
five_core_links = driver.find_elements(By.PARTIAL_LINK_TEXT, '5-core')

# Click on the first six "5-core" links to start the download
for link in five_core_links[1:5]:  # Only take the first six links
    href = link.get_attribute('href')
    driver.get(href)  # This will start the download
    time.sleep(1)  # Wait a bit for the download to initiate

# Close the browser after starting all downloads
time.sleep(20)
driver.quit()


