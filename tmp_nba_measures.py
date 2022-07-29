import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

pd.set_option('display.max_columns', 500)
SEASON = 2022


def get_nba_players_data(season):
    """
    Extracts all players details according to the season
    :param season:
    :return:
    """
    season_abrev = int(str(season)[2:])
    season_start = season - 1
    url = f'https://www.nba.com/stats/players/bio/?Season={season_start}-{season_abrev}&SeasonType=Regular%20Season'

    # have to download chrome driver according to your chrome version (https://chromedriver.chromium.org/downloads)
    try:
        driver = webdriver.Chrome('chromedriver.exe')
    except Exception as exc:
        print('Could not initialize chrome driver')
        raise Exception(exc)

    try:
        driver.get(url)
        driver.maximize_window()
    except Exception as exc:
        print(f'Could not open requested url: {url}')
        raise Exception(exc)

    xpath_dropdown_pages = '/html/body/main/div/div/div[2]/div/div/nba-stat-table/div[1]/div/div'
    try:
        dropdown_pages = WebDriverWait(driver, 1.5).until(EC.visibility_of_element_located((By.XPATH,
                                                                                            xpath_dropdown_pages)))
        num_pages = int(dropdown_pages.text.split()[-1])
    except Exception as exc:
        print(f'Could not get dropdown limit, xpath= {xpath_dropdown_pages}')
        raise Exception(exc)

    page_num = 1
    create_df = True
    while True:
        table_xpath = '/html/body/main/div/div/div[2]/div/div/nba-stat-table/div[2]/div[1]/table'
        try:
            table = WebDriverWait(driver, 1.5).until(EC.visibility_of_element_located((By.XPATH, table_xpath)))
            table_html = table.get_attribute('outerHTML')
            tmp_df = pd.read_html(table_html)[0]
        except Exception as exc:
            print(f'Could not fetch table on index {page_num} of {num_pages}, xpath = {table_xpath}')
            raise Exception(exc)

        if create_df:
            df = tmp_df
            create_df = False
        else:
            df = pd.concat([df, tmp_df])

        if page_num == num_pages:
            break
        else:
            next_xpath = '/html/body/main/div/div/div[2]/div/div/nba-stat-table/div[1]/div/div/a[2]'
            try:
                next_button = WebDriverWait(driver, 1.5).until(EC.visibility_of_element_located((By.XPATH, next_xpath)))
                next_button.click()
                page_num += 1
            except Exception as exc:
                print(f'Could not find button to pass to the next page , xpath = {table_xpath}')
                raise Exception(exc)

    driver.close()
    df = df[['Player', 'Team', 'Age', 'Height', 'Weight', 'College', 'Country', 'Draft Year', 'Draft Round',
             'Draft Number']]
    df = df.reset_index()
    df = df.drop(columns=['index'])
    return df


def get_player_id(SEASON):
    # TODO: instead of reading from player_ids.xlsx (excel file with two cols - player_id and player)
    # do it from executing functions from web_scrapping
    df_ids = pd.read_excel('player_ids.xlsx')
    df_data = get_nba_players_data(SEASON)
    df = pd.merge(df_data, df_ids, how='inner', left_on='Player', right_on='player') #inner joins get only rows with values on both tables
    df = df[['player_id', 'Team', 'Age', 'Height', 'Weight', 'College', 'Country', 'Draft Year', 'Draft Round',
             'Draft Number']]
    df = df.rename(columns={'Team': 'team', 'Age': 'age', 'Height': 'height', 'Weight': 'weight', 'College': 'college',
                            'Country': 'country', 'Draft Year': 'draft_year', 'Draft Round': 'draft_round',
                            'Draft Number': 'draft_number'})
    df.to_csv('players_bio.csv')


def main():
    try:
        get_player_id(SEASON)
    except Exception as exc:
        print('Exception found:', exc)


if __name__ == "__main__":
    main()
