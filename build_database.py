import pymysql
import config
import pvt_data_config
import pandas as pd
import logging

logging.basicConfig(filename='sql.log', encoding='utf-8', level=logging.INFO, format=config.LOG_FORMAT)


def create_connection(host, user, password):
    """
    Creates connection to SQL without specifying the database name
    Used only for creation of the database
    :param host: name of the host
    :param user: name of the user
    :param password: MySQL password
    :return: instance of a pymysql connection
    """
    return pymysql.connect(host=host,
                           user=user,
                           password=password,
                           cursorclass=pymysql.cursors.DictCursor)


def create_connection_db(host, user, password, database_name):
    """
    Creates connection to SQL with specified database
    :param host: name of the host
    :param user: name of the user
    :param password: MySQL password
    :param database_name: name of the database
    :return: instance of a pymysql connection
    """
    return pymysql.connect(host=host,
                           user=user,
                           password=password,
                           database=database_name,
                           cursorclass=pymysql.cursors.DictCursor)


def create_database(host, user, password, database_name):
    """
    Creates a new database
    :param host: name of the host
    :param user: name of the user
    :param password: MySQL password
    :param database_name: name of the database
    """
    connection = create_connection(host, user, password)
    with connection.cursor() as cursor:
        sql = f"CREATE DATABASE IF NOT EXISTS {database_name}"
        cursor.execute(sql)


def create_table_players(host, user, password, database_name):
    """
    Creates table 'players' in the database
    :param host: name of the host
    :param user: name of the user
    :param password: MySQL password
    :param database_name: name of the database
    """
    connection = create_connection_db(host, user, password, database_name)
    with connection.cursor() as cursor:
        sql = """CREATE TABLE IF NOT EXISTS players (
            player_id varchar(100) not null,
            name varchar(100),
            primary key (player_id))"""
        cursor.execute(sql)
    connection.commit()


def create_table_players_info(host, user, password, database_name):
    """
    Creates table 'players_info' in the database
    :param host: name of the host
    :param user: name of the user
    :param password: MySQL password
    :param database_name: name of the database
    """
    connection = create_connection_db(host, user, password, database_name)
    with connection.cursor() as cursor:
        sql = """CREATE TABLE IF NOT EXISTS players_info (
              player_id varchar(100) not null,
              team_id varchar(100),
              age int,
              height decimal(18,9),
              weight decimal(18,9),
              college varchar(100),
              country varchar(100),
              draft_year int,
              draft_round int,
              draft_number int,
              primary key (player_id))"""
        cursor.execute(sql)
    connection.commit()


def create_table_teams(host, user, password, database_name):
    """
    Creates table 'teams' in the database
    :param host: name of the host
    :param user: name of the user
    :param password: MySQL password
    :param database_name: name of the database
    """
    connection = create_connection_db(host, user, password, database_name)
    with connection.cursor() as cursor:
        sql = """CREATE TABLE IF NOT EXISTS teams (
              team_id varchar(100) not null,
              name varchar(100),
              primary key (team_id))"""
        cursor.execute(sql)
    connection.commit()


def create_table_stats_per_game(host, user, password, database_name):
    """
    Creates table 'stats_per_game' in the database
    :param host: name of the host
    :param user: name of the user
    :param password: MySQL password
    :param database_name: name of the database
    """
    connection = create_connection_db(host, user, password, database_name)
    with connection.cursor() as cursor:
        sql = """CREATE TABLE IF NOT EXISTS stats_per_game (
              player_id varchar(100) not null,
              pos varchar(100),
              team_season varchar(100),
              season int,
              g decimal(18,9),
              gs decimal(18,9),
              mp_per_g decimal(18,9),
              fg_per_g decimal(18,9),
              fga_per_g decimal(18,9),
              fg_pct decimal(18,9),
              fg3_per_g decimal(18,9),
              fg3a_per_g decimal(18,9),
              fg3_pct decimal(18,9),
              fg2_per_g decimal(18,9),
              fg2a_per_g decimal(18,9),
              fg2_pct decimal(18,9),
              efg_pct decimal(18,9),
              ft_per_g decimal(18,9),
              fta_per_g decimal(18,9),
              ft_pct decimal(18,9),
              orb_per_g decimal(18,9),
              drb_per_g decimal(18,9),
              trb_per_g decimal(18,9),
              ast_per_g decimal(18,9),
              stl_per_g decimal(18,9),
              blk_per_g decimal(18,9),
              tov_per_g decimal(18,9),
              pf_per_g decimal(18,9),
              pts_per_g decimal(18,9),
              foreign key (player_id) references players(player_id),
              foreign key (team_season) references teams(team_id))"""
        cursor.execute(sql)
    connection.commit()


def create_table_stats_per_minute(host, user, password, database_name):
    """
    Creates table 'stats_per_minute' in the database
    :param host: name of the host
    :param user: name of the user
    :param password: MySQL password
    :param database_name: name of the database
    """
    connection = create_connection_db(host, user, password, database_name)
    with connection.cursor() as cursor:
        sql = """CREATE TABLE IF NOT EXISTS stats_per_minute (
              player_id varchar(100) not null,
              pos varchar(100),
              team_season varchar(100),
              season int,
              g decimal(18,9),
              gs decimal(18,9),
              mp decimal(18,9),
              fg_per_mp decimal(18,9),
              fga_per_mp decimal(18,9),
              fg_pct decimal(18,9),
              fg3_per_mp decimal(18,9),
              fg3a_per_mp decimal(18,9),
              fg3_pct decimal(18,9),
              fg2_per_mp decimal(18,9),
              fg2a_per_mp decimal(18,9),
              fg2_pct decimal(18,9),
              ft_per_mp decimal(18,9),
              fta_per_mp decimal(18,9),
              ft_pct decimal(18,9),
              orb_per_mp decimal(18,9),
              drb_per_mp decimal(18,9),
              trb_per_mp decimal(18,9),
              ast_per_mp decimal(18,9),
              stl_per_mp decimal(18,9),
              blk_per_mp decimal(18,9),
              tov_per_mp decimal(18,9),
              pf_per_mp decimal(18,9),
              pts_per_mp decimal(18,9),
              foreign key (player_id) references players(player_id),
              foreign key (team_season) references teams(team_id))"""
        cursor.execute(sql)
    connection.commit()


def create_table_stats_per_poss(host, user, password, database_name):
    """
    Creates table 'stats_per_poss' in the database
    :param host: name of the host
    :param user: name of the user
    :param password: MySQL password
    :param database_name: name of the database
    """
    connection = create_connection_db(host, user, password, database_name)
    with connection.cursor() as cursor:
        sql = """CREATE TABLE IF NOT EXISTS stats_per_poss (
              player_id varchar(100) not null,
              pos varchar(100),
              team_season varchar(100),
              season int,
              g decimal(18,9),
              gs decimal(18,9),
              mp decimal(18,9),
              fg_per_poss decimal(18,9),
              fga_per_poss decimal(18,9),
              fg_pct decimal(18,9),
              fg3_per_poss decimal(18,9),
              fg3a_per_poss decimal(18,9),
              fg3_pct decimal(18,9),
              fg2_per_poss decimal(18,9),
              fg2a_per_poss decimal(18,9),
              fg2_pct decimal(18,9),
              ft_per_poss decimal(18,9),
              fta_per_poss decimal(18,9),
              ft_pct decimal(18,9),
              orb_per_poss decimal(18,9),
              drb_per_poss decimal(18,9),
              trb_per_poss decimal(18,9),
              ast_per_poss decimal(18,9),
              stl_per_poss decimal(18,9),
              blk_per_poss decimal(18,9),
              tov_per_poss decimal(18,9),
              pf_per_poss decimal(18,9),
              pts_per_poss decimal(18,9),
              off_rtg decimal(18,9),
              def_rtg decimal(18,9),
              foreign key (player_id) references players(player_id),
              foreign key (team_season) references teams(team_id))"""
        cursor.execute(sql)
    connection.commit()


def create_table_stats_totals(host, user, password, database_name):
    """
    Creates table 'stats_totals' in the database
    :param host: name of the host
    :param user: name of the user
    :param password: MySQL password
    :param database_name: name of the database
    """
    connection = create_connection_db(host, user, password, database_name)
    with connection.cursor() as cursor:
        sql = """CREATE TABLE IF NOT EXISTS stats_totals (
              player_id varchar(100) not null,
              pos varchar(100),
              team_season varchar(100),
              season int,
              g decimal(18,9),
              gs decimal(18,9),
              mp decimal(18,9),
              fg decimal(18,9),
              fga decimal(18,9),
              fg_pct decimal(18,9),
              fg3 decimal(18,9),
              fg3a decimal(18,9),
              fg3_pct decimal(18,9),
              fg2 decimal(18,9),
              fg2a decimal(18,9),
              fg2_pct decimal(18,9),
              efg_pct decimal(18,9),
              ft decimal(18,9),
              fta decimal(18,9),
              ft_pct decimal(18,9),
              orb decimal(18,9),
              drb decimal(18,9),
              trb decimal(18,9),
              ast decimal(18,9),
              stl decimal(18,9),
              blk decimal(18,9),
              tov decimal(18,9),
              pf decimal(18,9),
              pts decimal(18,9),
              foreign key (player_id) references players(player_id),
              foreign key (team_season) references teams(team_id))"""
        cursor.execute(sql)
    connection.commit()


def create_table_twitter_details(host, user, password, database_name):
    """
        Creates table 'twitter_details' in the database
        :param host: name of the host
        :param user: name of the user
        :param password: MySQL password
        :param database_name: name of the database
        """
    connection = create_connection_db(host, user, password, database_name)
    with connection.cursor() as cursor:
        sql = """CREATE TABLE IF NOT EXISTS twitter_details (
                  player_id varchar(100) not null,
                  creation_date date,
                  user_name varchar(100),
                  twitter_id varchar(100),
                  followers_count bigint,
                  following_count bigint,
                  tweet_count bigint,
                  listed_count bigint,
                  description varchar(500),
                  foreign key (player_id) references players(player_id))"""
        cursor.execute(sql)
    connection.commit()


def database_exists(host, user, password, database_name):
    """
    Checks if database exists
    :param database_name: name of database that the function will check if exists
    :return: True if database exists, else False
    """
    connection = create_connection(host, user, password)
    with connection.cursor() as cursor:
        sql = f"SHOW DATABASES"
    df = pd.read_sql(sql, connection)
    return True if database_name in df['Database'].tolist() else False


def table_exists(host, user, password, database_name, table_name):
    """
    Checks if database exists
    :param database_name: name of database that the function will check if exists
    :return: True if database exists, else False
    """
    connection = create_connection_db(host, user, password, database_name)
    with connection.cursor() as cursor:
        sql = f"SHOW TABLES"
    df = pd.read_sql(sql, connection)
    return True if table_name in df[f'Tables_in_{database_name}'].tolist() else False


def build_database_with_tables(host=pvt_data_config.HOST, user=pvt_data_config.USER, password=pvt_data_config.PASSWORD,
                               database_name=config.DATABASE_NAME):
    """
    Executes all commands to build the database schema for nba_data
    Created tables names: 'players', 'players_info', 'teams', 'stats_per_game', 'stats_per_minute',
    'stats_per_poss', 'stats_totals'
    :param host: name of the host
    :param user: name of the user
    :param password: MySQL password
    :param database_name: name of the database
    """
    # Checks if database exists before creating
    create_database(host=host, user=user, password=password, database_name=database_name)
    logging.info(f'Database created successfully')

    # Creates each table (if it exists or not is verified within the query)
    create_table_players(host=host, user=user, password=password, database_name=database_name)
    logging.info(f'Table players created successfully')

    create_table_players_info(host=host, user=user, password=password, database_name=database_name)
    logging.info(f'Table players_info created successfully')

    create_table_teams(host=host, user=user, password=password, database_name=database_name)
    logging.info(f'Table teams created successfully')

    create_table_stats_per_game(host=host, user=user, password=password, database_name=database_name)
    logging.info(f'Table stats_per_game created successfully')

    create_table_stats_per_minute(host=host, user=user, password=password, database_name=database_name)
    logging.info(f'Table stats_per_minute created successfully')

    create_table_stats_per_poss(host=host, user=user, password=password, database_name=database_name)
    logging.info(f'Table stats_per_poss created successfully')

    create_table_stats_totals(host=host, user=user, password=password, database_name=database_name)
    logging.info(f'Table stats_totals created successfully')

    create_table_twitter_details(host=host, user=user, password=password, database_name=database_name)
    logging.info(f'Table twitter_details created successfully')


if __name__ == "__main__":
    build_database_with_tables(host=pvt_data_config.HOST, user=pvt_data_config.USER, password=pvt_data_config.PASSWORD,
                               database_name=config.DATABASE_NAME)