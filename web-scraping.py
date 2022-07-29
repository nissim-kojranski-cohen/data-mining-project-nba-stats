from bs4 import BeautifulSoup
import requests
import csv
import config
import logging

# setting log configurations
logging.basicConfig(filename='nba_web_scrapping.log', encoding='utf-8', level=logging.INFO, format=config.LOG_FORMAT)


def get_html(year, stat_extension):
    url = ''.join([config.URL_BEG, str(year), stat_extension])
    response = requests.get(url)
    if response.status_code != 200:
        logging.critical(f'request status {response} for year {year} - NOT SUCCESSFUL!')
    else:
        logging.info(f'got request from main page for year {year}')

    try:
        logging.debug(f'parsing response url {url} for year {year}')
        soup = BeautifulSoup(response.text, "html.parser")
        table = soup.find_all(class_="full_table")
    except TypeError:
        logging.error(f'request status {response} for year {year} could not be parsed- NOT SUCCESSFUL!')
        raise TypeError("Could not parse request")
    except Exception as exc:
        logging.error(f'Not defined error for request status {response} for year {year} \n', exc)
        raise Exception(exc)

    if len(table) == 0:
        logging.error(f"Could not find table in the requested url: {url} for year {year}")
        raise TypeError(f"Could not find table in the requested url: {url} for year {year}")
    else:
        logging.info(f"Table returned successfully for url: {url} for year {year}")
        return table
        

def parse_html(response):
    list_of_dicts = []
    len_response = len(response)
    # iterating over soup object, each row contains one player stats
    for index, row in enumerate(response):
        # bool var to check if player's data exists
        got_player_data = True
        tmp_dict = {}
        # creating unique id for each player
        player_html = row.find_all("a")[0].get("href")
        player_id = player_html[:-5].split('/')[-1]
        tmp_dict['player_id'] = player_id
        # fetching data from table columns
        cols = row.find_all("td")
        if len(cols) == 0:
            logging.critical("Table contains no columns")
            raise TypeError("Could not find columns on html table")
        # saving data to temp dictionary
        for col in cols:
            try:
                tmp_dict[col.get("data-stat")] = col.text
            except Exception as exc:
                logging.error(f"Could not fetch data on index {index} out of {len_response}"
                              f" Details of exception: {exc}")
                got_player_data = False

        if got_player_data:
            logging.info(f"data for index {index} out of {len_response} "
                         f"player {tmp_dict['player']} fetched successfully")
            list_of_dicts.append(tmp_dict)
    return list_of_dicts


def export_data_to_csv(year, list_of_dicts, stat_type):
    if len(list_of_dicts) == 0 or list_of_dicts is None:
        logging.critical("List of dictionaries contain no data")
        raise TypeError("List of dictionaries contain no data")

    with open(f'sample_{year}{stat_type}.csv', 'w', newline='', encoding='utf-8') as file:
        csv_writer = csv.writer(file)
        write_header = True
        for dict_ in list_of_dicts:
            if write_header:
                csv_writer.writerow([key for key in dict_])
                write_header = False
            csv_writer.writerow([dict_[key] for key in dict_])
            logging.info(f"data for player {dict_['player']} saved successfully")


def main():
    # type of stats to scrape
    stat_extensions = [config.URL_END_TOTALS, config.URL_END_PERGAME, config.URL_END_PER36, config.URL_END_PER100POSS]
    # iterating over stat types
    for ext in stat_extensions:
        stat_type = ext.split('.')[0]
        # iterating over years
        for year in range(config.YEAR_START, config.YEAR_END + 1):
            print(f'Starting web scrapping for NBA players {ext[1:-5]} year {year}')
            try:
                # get url response
                html_response = get_html(year, ext)
                # parse data table into python list
                parsed_table = parse_html(html_response)
                # export the data to csv file
                export_data_to_csv(year, parsed_table, stat_type)
            except Exception as exc:
                print('Exception found:', exc)
            print(f'Web scrapping for NBA players {ext[1:-5]} year {year} completed successfully')


if __name__ == "__main__":
    main()
