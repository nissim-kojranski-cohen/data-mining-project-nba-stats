import logging
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import config
from web_scraping_players_stats import get_html, parse_html


pd.set_option('display.max_columns', 500)
logging.basicConfig(filename='nba_web_scrapping.log', encoding='utf-8', level=logging.INFO, format=config.LOG_FORMAT)


def convert_feet_inches_to_cm(feet_inches):
    """ Converts str(feet-inches) to cm"""
    try:
        feet, inches = feet_inches.split('-')
        return int(feet) * config.FEET_TO_CM + int(inches) * config.INCHES_TO_CM
    except ValueError:
        logging.error(f'ValueError occurred when trying to convert {feet_inches} to cm')
        return ''
    except TypeError:
        logging.error(f'TypeError occurred when trying to convert {feet_inches} to cm')
        return ''


def convert_pounds_to_kg(pounds):
    """ Converts pounds to cm"""
    try:
        return pounds * config.POUNDS_TO_KG
    except ValueError:
        logging.error(f'ValueError occurred when trying to convert {pounds} to kg')
        return ''
    except TypeError:
        logging.error(f'TypeError occurred when trying to convert {pounds} to kg')
        return ''


def get_nba_players_ids(season):
    """
    Gets NBA players ids on Basketball Reference Website (primary key used for players in the project)
    :param season:
    :return: DataFrame with 2 columns: player_id and player (name of the player)
    """
    logging.debug(f'Extracting players ids from Basketball Reference Website')
    html_ids = get_html(season, config.URL_END_TOTALS)
    parsed_ids = parse_html(html_ids)
    df_ids = pd.DataFrame(parsed_ids)
    df_ids = df_ids[['player_id', 'player']]
    logging.info(f'Successfully extracted players ids from Basketball Reference Website')
    return df_ids


def get_nba_players_data(season, df_ids):
    """
    Extracts all players details according to the season
    :param season: year of the season to get the players information
    :param df_ids: DataFrame with 2 columns: player_id and player (name of the player)
    :return:
    """
    season_abrev = int(str(season)[2:])
    season_start = season - 1
    logging.debug(f'Opening NBA website to extract players information for season {season_start}/{season_abrev}')
    url = f'https://www.nba.com/stats/players/bio/?Season={season_start}-{season_abrev}&SeasonType=Regular%20Season'
    logging.debug(f'NBA url address: {url}')

    # have to download chrome driver according to your chrome version (https://chromedriver.chromium.org/downloads)
    try:
        driver = webdriver.Chrome('chromedriver.exe')
    except Exception as exc:
        print('ERROR: Could not initialize chrome driver')
        logging.critical(f'Could not initialize chrome driver, program will be terminated')
        raise Exception(exc)

    try:
        driver.get(url)
        driver.maximize_window()
        logging.info(f'Opened url {url} successfully')
    except Exception as exc:
        print(f'Could not open requested url: {url}')
        logging.critical(f'Could not open requested url: {url}, program will be terminated')
        raise Exception(exc)

    xpath_dropdown_pages = '/html/body/main/div/div/div[2]/div/div/nba-stat-table/div[1]/div/div'
    try:
        dropdown_pages = WebDriverWait(driver, config.WAITING_TIME_SELENIUM).until(
            EC.visibility_of_element_located((By.XPATH,
                                              xpath_dropdown_pages)))
        num_pages = int(dropdown_pages.text.split()[-1])
        logging.info(f'Found number of pages: {num_pages}, and found dropdown item on webpage')
    except Exception as exc:
        print(f'Could not get dropdown limit, xpath= {xpath_dropdown_pages}')
        logging.critical(f'Could not get dropdown limit, xpath= {xpath_dropdown_pages}, program will be terminated')
        raise Exception(exc)

    page_num = 1
    create_df = True
    while True:
        table_xpath = '/html/body/main/div/div/div[2]/div/div/nba-stat-table/div[2]/div[1]/table'
        try:
            table = WebDriverWait(driver, config.WAITING_TIME_SELENIUM).until(
                EC.visibility_of_element_located((By.XPATH, table_xpath)))
            table_html = table.get_attribute('outerHTML')
            tmp_df = pd.read_html(table_html)[0]
            logging.info(f'Table with players details read successfully')
        except Exception as exc:
            print(f'Could not fetch table on index {page_num} of {num_pages}, xpath = {table_xpath}')
            logging.critical(f'Could not fetch table on index {page_num} of {num_pages}, xpath = {table_xpath}, '
                             f'program will be terminated')
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
                next_button = WebDriverWait(driver, config.WAITING_TIME_SELENIUM).until(
                    EC.visibility_of_element_located((By.XPATH, next_xpath)))
                next_button.click()
                page_num += 1
                logging.info(f'Could pass to next page on NBA website in order to get players info')
            except Exception as exc:
                print(f'Could not find button to pass to the next page , xpath = {table_xpath}')
                logging.critical(f'Could not pass to next page on NBA website in order to get players info, '
                                 f'program will be terminated')
                raise Exception(exc)

    driver.close()
    df = df.reset_index()
    df = df.drop(columns=['index'])

    # joins with players ids
    # inner joins get only rows with values on both tables
    df = pd.merge(df, df_ids, how='inner', left_on='Player', right_on='player')
    df = df[['player_id', 'Team', 'Age', 'Height', 'Weight', 'College',
             'Country', 'Draft Year', 'Draft Round','Draft Number']]
    df = df.rename(columns={'Team': 'team', 'Age': 'age', 'Height': 'height', 'Weight': 'weight', 'College': 'college',
                            'Country': 'country', 'Draft Year': 'draft_year', 'Draft Round': 'draft_round',
                            'Draft Number': 'draft_number'})
    df['height'] = df.apply(lambda x: convert_feet_inches_to_cm(x['height']), axis=1)
    df['weight'] = df.apply(lambda x: convert_pounds_to_kg(x['weight']), axis=1)
    return df


def export_players_info(year_start=config.YEAR_START, year_end=config.YEAR_END):
    create_dfs = True
    for year in range(year_start, year_end + 1):
        print(f'Starting to run for {year}')
        if create_dfs:
            df_ids = get_nba_players_ids(year)
            df_nba_players_info = get_nba_players_data(year, df_ids)
            create_dfs = False
        else:
            tmp_df_ids = get_nba_players_ids(year)
            tmp_df_nba_players_info = get_nba_players_data(year, df_ids)
            df_nba_players_info = pd.concat([df_nba_players_info, tmp_df_nba_players_info])
            df_ids = pd.concat([df_ids, tmp_df_ids])
            df_nba_players_info = df_nba_players_info.drop_duplicates('player_id', keep='last')
            df_ids = df_ids.drop_duplicates('player_id', keep='last')

    df_nba_players_info.to_csv('players_info.csv', index=None)
    df_ids.to_csv('players_id.csv', index=None)


if __name__ == "__main__":
    export_players_info()
