import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

pd.set_option('display.max_columns', 500)
url = 'https://www.nba.com/stats/players/bio/?Season=2021-22&SeasonType=Regular%20Season'

driver = webdriver.Chrome(
    'chromedriver.exe')  # have to download chrome driver according to your chrome version (https://chromedriver.chromium.org/downloads)
driver.get(url)
driver.maximize_window()

# Change to get number of pages automatically --still couldnt do it
'''
xpath_dropdown_pages = '/html/body/main/div/div/div[2]/div/div/nba-stat-table/div[1]/div/div/select'
dropdown_pages = WebDriverWait(driver, 1.5).until(EC.visibility_of_element_located((By.XPATH, xpath_dropdown_pages)))
print(dir(xpath_dropdown_pages))
print(type(dropdown_pages))
print(dropdown_pages.find('option'))
'''
num_pages = 13
page_num = 1
create_df = True

while True:
    table_xpath = '/html/body/main/div/div/div[2]/div/div/nba-stat-table/div[2]/div[1]/table'
    table = WebDriverWait(driver, 1.5).until(EC.visibility_of_element_located((By.XPATH, table_xpath)))
    table_html = table.get_attribute('outerHTML')
    tmp_df = pd.read_html(table_html)[0]
    if create_df:
        df = tmp_df
        create_df = False
    else:
        df = pd.concat([df, tmp_df])

    if page_num == num_pages:
        break
    else:
        next_xpath = '/html/body/main/div/div/div[2]/div/div/nba-stat-table/div[1]/div/div/a[2]'
        next_button = WebDriverWait(driver, 1.5).until(EC.visibility_of_element_located((By.XPATH, next_xpath)))
        next_button.click()
        page_num += 1

driver.close()
df = df[['Player', 'Team', 'Age', 'Height', 'Weight', 'College', 'Country', 'Draft Year', 'Draft Round', 'Draft Number']]
df = df.reset_index()
df = df.drop(columns=['index'])
df.to_csv('players_bio.csv')
