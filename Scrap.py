# %%
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
import csv
import os

# %%
## Set Options
chrome_options = Options()
chrome_options.add_argument('--headless')
chrome_options.add_argument("--window-size=1920,1080")
chrome_options.add_argument('--disable-cache')
chrome_options.add_argument('--incognito')

## Set Service
service = Service()

## Set Driver
driver = webdriver.Chrome(service=service, options=chrome_options)

## Set Base URL
base_url = "https://novelpia.com/novel_ranking"

# %%
## Move to Base URL
driver.get(base_url)
driver.implicitly_wait(10)

## Get Date
date = driver.find_element(By.XPATH, "//*[(@class='time regTime')]").text.replace('/', '-')
print(f"Date of Ranking : {date}")

## Get Novel List
novel_list = driver.find_elements(By.XPATH, "//*[contains(@class, 'novel-title')]")
N = len(novel_list) # Number of Novels (must be 100)

if (N != 100):
    raise ValueError(f"Error: N must be 100. Current value is {N}.") # Raise Error if N is Not 100

## Set CSV File Path
dir_path = os.getcwd()
csv_file_name = f"{date}.csv"
file_path = f"{dir_path}{os.sep}data{os.sep}{csv_file_name}"

## Prepare Data List
raw_data = [{} for _ in range(N)]

for i in range(N):
    novel_list = driver.find_elements(By.XPATH, "//*[contains(@class, 'novel-title')]") # Repeat for Every Iter
    
    if (len(novel_list) != 100):
        raise ValueError(f"Error: Number of novels must be 100. Current value is {len(novel_list)}.") # Raise Error if # of novels is Not 100
    
    ## Move to New Page
    new_page = novel_list[i].click()
    driver.implicitly_wait(10)
    
    ## Get Datas
    data = raw_data[i] # Assign a Dictionary
    new_url = driver.current_url # Current URL
    fav, alr, eps = driver.find_elements(By.XPATH, "//*[(@class='info-count2')]//*[(@class='writer-name')]")
    tags = driver.find_elements(By.XPATH, "//*[(@class='ep-info-line epnew-tag')]//*[(@class='tag')]")
    
    data['Date'] = date
    data['Ranking'] = i + 1
    data['ID'] = int(new_url.split('/')[-1])
    data['Title'] = driver.find_element(By.XPATH, "//*[contains(@class, 'epnew-novel-title')]").text
    data['Author'] = driver.find_element(By.XPATH, "//a[(@class='writer-name')]").text
    data['Fav'] = int(fav.text.replace(',', ''))
    data['Alr'] = int(alr.text.replace(',', ''))
    data['Eps'] = int(eps.text[:-2].replace(',', ''))
    data['Tags'] = []
    
    for tag in tags:
        data['Tags'].append(tag.text[1:]) # Remove a Hashtag
        
    print(f"Currently at {new_url}. Progress : {i+1}/{N}")
    
    ## Return to Base URL
    driver.get(base_url)
    driver.implicitly_wait(10)

print(f"Data has been prepared.")

## Save Data into CSV File
with open(file_path, mode='w', newline='') as file:
    fields = ['Date', 'Ranking', 'ID', 'Title', 'Author', 'Fav', 'Alr', 'Eps', 'Tags']
    writer = csv.DictWriter(file, fieldnames=fields)
    writer.writeheader()
    writer.writerows(raw_data)

print(f"Data has been saved to {csv_file_name}")

driver.quit()

# %%
import pandas as pd
import json
from google.cloud import bigquery

## Export Data From JSON File
with open('config.json', 'r') as file:
    config = json.load(file)

## Set table_ref
projectId = config['projectId']
datasetId = config['datasetId']
tableId = config['tableId']
table_ref = f"{projectId}.{datasetId}.{tableId}"

## Set Configs
job_config = bigquery.LoadJobConfig()
job_config.source_format = bigquery.SourceFormat.CSV
job_config.skip_leading_rows = 1
job_config.autodetect = True
job_config.encoding="UTF-8"

## Convert CSV File into UTF-8
df = pd.read_csv(file_path, encoding="CP949")  # Original Encoding is CP949
utf8_path = f"{file_path}_utf8.csv" # Temporary File
df.to_csv(utf8_path, encoding="utf-8", index=False)

## Open BigQuery Client
client = bigquery.Client()

## Load CSV File into the Table
with open(utf8_path, 'rb') as source_file:
    job = client.load_table_from_file(source_file, table_ref, job_config=job_config)
    job.result()

## SQL Query
query = f"""
SELECT COUNT(*) AS NUM_ROWS
FROM `{table_ref}`
WHERE Date = '{date}'
"""

## Execute Query
query_job = client.query(query)
results = query_job.result()

## Print Result
for row in results:
    print(f"Loaded {row.NUM_ROWS} rows into {projectId}.{datasetId}.{tableId}")

## Remove Temporary File
os.remove(utf8_path)