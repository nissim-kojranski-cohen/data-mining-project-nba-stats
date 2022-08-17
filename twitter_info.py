import requests
from bs4 import BeautifulSoup
import csv
import config
import logging
import os
import json
import pandas as pd

TOKEN = "AAAAAAAAAAAAAAAAAAAAAFj9fwEAAAAALn3C3fhbxF9eFnPtE13yL5LLjI8%3D9gxlzjWtbsXOn8qN5367rbQ9iggDy170crEEvC0eRsA1n9QQ1l"
bearer_token = TOKEN


def create_url(users):
    # Specify the usernames that you want to lookup below
    # You can enter up to 100 comma-separated values.
    users = ','.join(users)
    usernames = f"usernames={users}"
    user_fields = "user.fields=description,created_at,public_metrics"
    # User fields are adjustable, options include:
    # created_at, description, entities, id, location, name,
    # pinned_tweet_id, profile_image_url, protected,
    # public_metrics, url, username, verified, and withheld
    url = "https://api.twitter.com/2/users/by?{}&{}".format(usernames, user_fields)
    return url


def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2UserLookupPython"
    return r


def connect_to_endpoint(url):
    response = requests.request("GET", url, auth=bearer_oauth,)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(
            "Request returned an error: {} {}".format(
                response.status_code, response.text
            )
        )
    return response.json()


def return_player_json(users):
    url = create_url(users)
    json_response = connect_to_endpoint(url)
    #print(json.dumps(json_response, indent=4, sort_keys=True))
    return json_response

def return_player_df(users):
    player_json = return_player_json(users)
    players_df = pd.DataFrame(columns=['creation_date', 'user_name', 'twitter_id', 'followers_count',
                                      'following_count', 'tweet_count', 'listed_count', 'description'])
    for index, dict_ in enumerate(player_json['data']):

        creation_date = dict_['created_at']
        user_name = dict_['username']
        twitter_id = dict_['id']
        followers_count = dict_['public_metrics']['followers_count']
        following_count = dict_['public_metrics']['following_count']
        tweet_count = dict_['public_metrics']['tweet_count']
        listed_count = dict_['public_metrics']['listed_count']
        description = dict_['description']
        players_df.loc[index] = [creation_date, user_name, twitter_id, followers_count,
                                following_count, tweet_count, listed_count, description]
    return players_df

def get_all_players_id_web():
    """OFFLINE"""
    """Returns table with players' name and twitter ID"""
    df_players = pd.read_html('https://www.basketball-reference.com/friv/twitter.html')
    df_players = df_players[0]
    df_players = df_players[df_players['Twitter'] != '']
    df_players = df_players[~df_players['Twitter'].isna()]
    return df_players


def get_all_players_id_file():
    """Returns table with players' unique id"""



def twitter_accounts():
    """
    creates a csv file with players twitter accounts url extension
    :return: None
    """
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


if __name__ == "__main__":
    df_info = pd.DataFrame(columns=['creation_date', 'user_name', 'twitter_id', 'followers_count',
                                      'following_count', 'tweet_count', 'listed_count', 'description'])
    players = get_all_players_id()
    twitter_accounts = players['Twitter'].tolist()
    limit = 100
    start = 0
    end = limit

    while True:
        print(start, end)
        users = twitter_accounts[start:end]
        if len(users) == 0:
            break
        players_df = return_player_df(users)
        df_info = pd.concat([df_info, players_df])

        start += limit
        end += limit

    df_info.to_csv('twitter_details2.csv')
