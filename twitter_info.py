import requests
import os
import json
import pandas as pd
pd.set_option('display.max_columns', 500)

# To set your enviornment variables in your terminal run the following line:
# export 'BEARER_TOKEN'='<your_bearer_token>'
TOKEN = "AAAAAAAAAAAAAAAAAAAAAFj9fwEAAAAAdVHgGmMH56%2BloRMa0HkKXGV%2FW5E%3DxRXj73PMqXSpFNjQFFlAaKiIA8mGKa17n2DIfWxtjhL0kquEj2"
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

def get_all_players_id():
    df_players = pd.read_html('https://www.basketball-reference.com/friv/twitter.html')
    df_players = df_players[0]
    df_players = df_players[df_players['Twitter'] != '']
    df_players = df_players[~df_players['Twitter'].isna()]
    return df_players

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