import requests
import os
from bs4 import BeautifulSoup
from urllib.parse import quote, urlparse, urlunparse

# Create data dir
DATA_DIR = "_temp"
os.makedirs(DATA_DIR, exist_ok=True) 

headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept-Language': 'en-US,en;q=0.9',
}

r = requests.get('https://insideairbnb.com/get-the-data/', headers=headers)

file_path = f'{DATA_DIR}/insideairbnb.html'
with open(file_path, 'wb') as file: # 🟢 'wb' for Write Binary
    file.write(r.content)

with open(file_path, 'r', encoding='utf-8') as file:
    html_content = file.read()

soup = BeautifulSoup(html_content, 'html.parser')

results = []


for header in soup.find_all('h3'):
    head = header.get_text(strip=True)
    table = header.find_next_sibling('table')
    
    result_buffer = {}
    if table:
        listings_link = table.find('a', href=lambda h: h and 'listings.csv.gz' in h)
        if listings_link:
            raw_url = listings_link['href']
            p = urlparse(raw_url)
            safe_path = quote(p.path, safe='/')
            result_buffer["listing_url"] = urlunparse((p.scheme, p.netloc, safe_path, p.params, p.query, p.fragment))
            result_buffer["header"] = head

        reviews_link = table.find('a', href=lambda h: h and 'reviews.csv.gz' in h)
        if reviews_link:
            raw_url = reviews_link['href']
            p = urlparse(raw_url)
            safe_path = quote(p.path, safe='/')
            result_buffer["review_url"] = urlunparse((p.scheme, p.netloc, safe_path, p.params, p.query, p.fragment))

        results.append(result_buffer)

for r in results:
    split = r['header'].split(',')
    country = split[-1]
    if len(split) == 3:
        city = split[0]
        province = split[1]
    elif len(split) > 3:
        city = split[0]
        province = '-'.join(split[0:-1])
    elif len(split) == 1:
        city = None
        province = None
    elif len(split) == 2:
        city = split[0]
        province = None
    else:
        city = None
        province = None
    r['city'] = city
    r['province'] = province
    r['country'] = country


import json
output_path = f'{DATA_DIR}/insideairbnb.json'
with open(output_path, 'w') as f:
    json.dump(results, f)