from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.remote.errorhandler import MoveTargetOutOfBoundsException
from selenium.common.exceptions import NoSuchElementException
import httplib2shim
from google_auth_httplib2 import AuthorizedHttp

import re
import os

from google.cloud import storage
from google.oauth2 import service_account

import json
import pygsheets
from datetime import datetime
from time import sleep
import pandas as pd
import numpy as np

SCOPES = [
    "https://www.googleapis.com/auth/cloud-platform",
]

SERVICE_ACCOUNT_JSON = json.loads(os.environ['GCP_SERVICE_ACCOUNT'], strict= False)

gcp_credentials = service_account.Credentials.from_service_account_info(SERVICE_ACCOUNT_JSON, scopes=SCOPES)
storage_client = storage.Client(project = 'gcp-project-365616', credentials = gcp_credentials)
bucket = storage_client.get_bucket('amazon_scrapper')

blob = bucket.blob('cookies.json')

print('Storage ok!')

http = httplib2shim.Http()
client = pygsheets.authorize(service_account_env_var = 'GCP_SERVICE_ACCOUNT', http = http)

ss = client.open_by_url('https://docs.google.com/spreadsheets/d/1j-1C5fcGCxZCT2ZsgQKOxkyEjNsNeYChsTvzxQWpgvE/edit#gid=0')
print('Ss openned!')

with blob.open() as f:
    cookies = json.loads(f.read())

ffOptions = Options()
ffOptions.add_argument("-headless")

driver = webdriver.Firefox(options=ffOptions)

driver.get("https://www.amazon.com.br")

for cookie in cookies:
    driver.add_cookie(cookie)

driver.get("https://www.amazon.com.br/hz/wishlist/ls/?ref=cm_wl_your_lists")
assert "Amazon.com.br" in driver.title
print('Logged in!')

actions_chains = webdriver.ActionChains(driver)

with blob.open("w") as f:
    f.write(json.dumps(driver.get_cookies()))

fully_loaded = False 

while not fully_loaded:
    try:
        nav_footer = driver.find_element(By.ID, 'navFooter')
        actions_chains.scroll_to_element(nav_footer).perform()
        sleep(2)
        fully_loaded = True
    except MoveTargetOutOfBoundsException:
        actions_chains.scroll_by_amount(0, 2000).perform()
        sleep(1)

elements = driver.find_elements(By.CSS_SELECTOR,'li[data-id="2GU2ZQQOILS7P"]')

new_data = []

now = datetime.now()
now_str = now.strftime('%Y-%m-%d')

for item in elements:
    try:
        texto_frete = item.find_element(By.CSS_SELECTOR, 'span[class="a-color-secondary a-size-base"]').text
        preco_frete = re.sub(r'[^\d,]', '', texto_frete)
        preco_frete = float(preco_frete.replace(',', '.'))

    except NoSuchElementException:
        preco_frete = 0

    new_data.append([
        now_str,
        item.get_attribute('data-itemid'),
        item.find_element(By.CSS_SELECTOR, 'a[class="a-link-normal"]').get_attribute('title'),
        float(item.get_attribute('data-price')) + preco_frete
    ])

print('Data extracted!')

ws = ss.worksheet()
df = ws.get_as_df()

if now_str in df['Date'].unique():
    df = df[df['Date'] != now_str]
    df_new_data = pd.DataFrame(new_data, columns=df.columns)
    df_new_data = df_new_data.replace([np.inf, -np.inf], 0)
    df = pd.concat([df_new_data, df])
    print(f'Updating {len(new_data)} rows')
    ws.set_dataframe(
        df,
        'A1'
    )

else:
    df_new_data = pd.DataFrame(new_data, columns=df.columns)
    df_new_data = df_new_data.replace([np.inf, -np.inf], 0)
    print(f'Inserting {len(new_data)} rows')
    ws.insert_rows(
        1,
        len(df_new_data),
        df_new_data.values.tolist(),
    )

driver.close()
print('Success!')