import requests
from bs4 import BeautifulSoup
import csv
import config
import os
import json
import pandas as pd
import logging
import config
import pvt_data_config

logging.basicConfig(filename='nba_twitter_api.log', encoding='utf-8', level=logging.INFO, format=config.LOG_FORMAT)


def create_url(users):
    """ Returns URL that will be used to request players details in the twitter API """
    # asserts do not exceed api limit
    assert len(users) <= config.TWITTER_USERS_LIMIT_API

    users = ','.join(users)
    usernames = f"usernames={users}"
    user_fields = "user.fields=description,created_at,public_metrics"
    url = "https://api.twitter.com/2/users/by?{}&{}".format(usernames, user_fields)
    logging.info(f"URL for players {users} created successfully")
    return url


def bearer_oauth(r):
    """ Returns the Bearer token authentication """
    r.headers["Authorization"] = f"Bearer {pvt_data_config.TWITTER_BEARER_TOKEN}"
    r.headers["User-Agent"] = "v2UserLookupPython"
    logging.info("Bearer token authentication created successfully")
    return r


def connect_to_endpoint(url):
    """ Connects to API endpoint """
    response = requests.request("GET", url, auth=bearer_oauth, )
    logging.debug(f"Response status code is {response.status_code} for url {url}")
    if response.status_code != 200:
        logging.critical(f"Request failed for url {url}")
        raise Exception(f"Request returned an error: {response.status_code} {response.text}")
    else:
        logging.info(f"Request succeeded for url {url}")
        return response.json()


def return_player_json(users):
    """ Returns json with players details """
    url = create_url(users)
    json_response = connect_to_endpoint(url)
    logging.info(f'json for users {users} generated successfully')
    return json_response


def return_player_df(users, players_dict):
    """ Converts the json response into a pandas dataframe object """
    player_json = return_player_json(users)

    players_df = pd.DataFrame(columns=['player_id', 'creation_date', 'user_name', 'twitter_id', 'followers_count',
                                       'following_count', 'tweet_count', 'listed_count', 'description'])
    for index, dict_ in enumerate(player_json['data']):
        # extracts data from the json file
        creation_date = dict_['created_at'][:10]
        user_name = dict_['username']
        twitter_id = dict_['id']
        followers_count = dict_['public_metrics']['followers_count']
        following_count = dict_['public_metrics']['following_count']
        tweet_count = dict_['public_metrics']['tweet_count']
        listed_count = dict_['public_metrics']['listed_count']
        description = dict_['description']

        # json might come out of order, we use the dictionary to get the player id for the equivalent twitter account

        player_id = players_dict[user_name.lower()]

        players_df.loc[index] = [player_id, creation_date, user_name, twitter_id, followers_count,
                                 following_count, tweet_count, listed_count, description]
    return players_df


def _get_all_players_id_web():
    """ Returns table with players' name and twitter ID """
    """ OFFLINE - using get_all_players_id_file instead, keeping this one for future use """
    df_players = pd.read_html('https://www.basketball-reference.com/friv/twitter.html')
    df_players = df_players[0]
    df_players = df_players[df_players['Twitter'] != '']
    df_players = df_players[~df_players['Twitter'].isna()]
    return df_players


def get_twitter_accounts():
    """ Creates a csv file with players_id and players twitter account name """
    # get request of twitter info from Basketball-Reference.com
    url = config.TWITTER_ADDRESSED_LIST
    response = requests.get(url)
    if response.status_code != 200:
        logging.critical(f'request status {response} for players twitter info - NOT SUCCESSFUL!')
    else:
        logging.info(f'got request from players twitter info')

    try:
        logging.debug(f'parsing response url {url} for players twitter info')
        soup = BeautifulSoup(response.text, "html.parser")
        # fetch the table with the info
        table = soup.find(class_='sortable stats_table')
    except TypeError:
        logging.error(f'request status {response} for players twitter info could not be parsed- NOT SUCCESSFUL!')
        raise TypeError("Could not parse request")
    except Exception as exc:
        logging.error(f'Not defined error for request status {response} for players twitter info \n', exc)
        raise Exception(exc)

    if len(table) == 0:
        logging.error(f"Could not find table in the requested url: {url} for players twitter info")
        raise TypeError(f"Could not find table in the requested url: {url} for players twitter info")
    else:
        logging.info(f"Table returned successfully for url: {url} for players twitter info")

    # create a list to store the info
    player_twitter = []
    # iterate over the table, each iteration is one player's info, store data to player_twitter
    for i, row in enumerate(table.find('tbody').find_all('tr')):
        temp_list = []
        try:
            for a_tag in row.find_all('a'):
                temp_list.append(a_tag.get('href').split('/')[-1].split('.')[0])
        except Exception as exc:
            logging.error(f"Could not fetch data on row {i}, Details of exception: {exc}")
        player_twitter.append(temp_list)

    # save data from player_twitter to twitter_addresses.csv
    with open(f'twitter_addresses.csv', 'w', newline='', encoding='utf-8') as file:
        csv_writer = csv.writer(file)
        # columns names
        csv_writer.writerow(['player_id', 'twitter_address'])
        for row in player_twitter:
            try:
                csv_writer.writerow([cell for cell in row])
                logging.info(f"data for player {row[0]} saved successfully")
            except Exception as exc:
                logging.error(f"Could not save data of player {row[0]}, Details of exception: {exc}")


def get_all_players_id_file():
    """Returns table with players' unique id"""
    # runs function that generates the csv file
    try:
        get_twitter_accounts()
    except Exception as exc:
        logging.critical(f"Could not generate file with players twitter accounts, exception: {exc}")

    # reads the csv file, filters for empty/null values
    filename = 'twitter_addresses.csv'
    if filename in os.listdir():
        df_players = pd.read_csv(filename)
        df_players = df_players[df_players['twitter_address'] != '']
        df_players = df_players[~df_players['twitter_address'].isna()]
        logging.info(f"File with players twitter details was read successfully")
        return df_players
    else:
        logging.critical(f"Could not generate file with players twitter accounts")


def df_to_dict(df, reverse_order=False, lowercase_all=True):
    """ Transforms a 2 column dataframe (col1, col2) into a dictionary with {col1: col2}"""
    if not reverse_order:
        col1 = df.columns[0]
        col2 = df.columns[1]
    else:
        col1 = df.columns[1]
        col2 = df.columns[0]

    dict_ = {}
    for index, row in df.iterrows():
        if lowercase_all:
            dict_[row[col1].lower()] = row[col2.lower()]
        else:
            dict_[row[col1]] = row[col2]
    return dict_


def export_players_twitter_data():

    try:
        players = get_all_players_id_file()
        twitter_ids = players['twitter_address'].tolist()
        players_dict = df_to_dict(players, reverse_order=True)
    except Exception as exc:
        logging.critical(f'Could not retrieve players twitter accounts, exception {exc}')

    limit = config.TWITTER_USERS_LIMIT_API
    start = 0
    end = limit

    df_info = pd.DataFrame(columns=['player_id', 'creation_date', 'user_name', 'twitter_id', 'followers_count',
                                    'following_count', 'tweet_count', 'listed_count', 'description'])

    while True:
        # players that we'll do the request for
        users = twitter_ids[start:end]
        if len(users) == 0:
            break

        try:
            players_df = return_player_df(users, players_dict)
            df_info = pd.concat([df_info, players_df])
        except Exception as exc:
            logging.critical(f'Could not generate dataframe with the info for players {users}, exception {exc}')
            break

        start += limit
        end += limit

    try:
        df_info = df_info.drop_duplicates()
        df_info.to_csv('twitter_details.csv', index=None)
        logging.info('File with twitter data for players exported successfully')
    except Exception as exc:
        logging.critical(f'Could not export file with players twitter details, exception {exc}')
        raise Exception(exc)


if __name__ == "__main__":
    try:
        export_players_twitter_data()
    except Exception as exc:
        logging.error(f'Could not generate file with players twitter data, exception {exc}')


