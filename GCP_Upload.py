import requests
from bs4 import BeautifulSoup

# URL of the website page with the data links
url = 'https://cseweb.ucsd.edu/~jmcauley/datasets/amazon_v2/#subsets'

# Fetch the page
response = requests.get(url)
soup = BeautifulSoup(response.text, 'html.parser')

# Assuming data links are in <a> tags, customize this based on the actual structure
data_links = [a['href'] for a in soup.find_all('a', href=True) if 'json.gz' in a['href']]
print(data_links )
