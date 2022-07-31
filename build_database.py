import pymysql
import config
import sql_config
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
        sql = f"CREATE DATABASE {database_name}"
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
        sql = """CREATE TABLE players (
            player_id varchar(100) primary key,
            name varchar(100))"""
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
        sql = """CREATE TABLE players_info (
              player_id varchar(100),
              team_id varchar(100),
              age int,
              height decimal(18,9),
              weight decimal(18,9),
              college varchar(100),
              country varchar(100),
              draft_year int,
              draft_round int,
              draft_number int)"""
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
        sql = """CREATE TABLE teams (
              team_id varchar(100),
              name varchar(100))"""
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
        sql = """CREATE TABLE stats_per_game (
              player_id varchar(100),
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
              pts_per_g decimal(18,9))"""
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
        sql = """CREATE TABLE stats_per_minute (
              player_id varchar(100),
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
              pts_per_mp decimal(18,9))"""
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
        sql = """CREATE TABLE stats_per_poss (
              player_id varchar(100),
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
              def_rtg decimal(18,9))"""
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
        sql = """CREATE TABLE stats_totals (
              player_id varchar(100),
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
              pts decimal(18,9))"""
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


def build_database_with_tables(host=sql_config.HOST, user=sql_config.USER, password=sql_config.PASSWORD,
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
    if database_exists(host=host, user=user, password=password, database_name=database_name):
        logging.info(f'Database {database_name} already exists so it will not be created')
    else:
        logging.info(f'Database {database_name} does not exist so it will be created')
        create_database(host=host, user=user, password=password, database_name=database_name)
        logging.info(f'Database {database_name} created successfully')

    # For each table check if exists before creating
    if table_exists(host=host, user=user, password=password, database_name=database_name, table_name='players'):
        logging.info(f'Table players already exists so it will not be created')
    else:
        logging.info(f'Table players already exists will be created')
        create_table_players(host=host, user=user, password=password, database_name=database_name)
        logging.info(f'Table players created successfully')

    if table_exists(host=host, user=user, password=password, database_name=database_name, table_name='players_info'):
        logging.info(f'Table players_info already exists so it will not be created')
    else:
        logging.info(f'Table players_info already exists will be created')
        create_table_players_info(host=host, user=user, password=password, database_name=database_name)
        logging.info(f'Table players_info created successfully')

    if table_exists(host=host, user=user, password=password, database_name=database_name, table_name='teams'):
        logging.info(f'Table teams already exists so it will not be created')
    else:
        logging.info(f'Table teams already exists will be created')
        create_table_teams(host=host, user=user, password=password, database_name=database_name)
        logging.info(f'Table teams created successfully')

    if table_exists(host=host, user=user, password=password, database_name=database_name, table_name='stats_per_game'):
        logging.info(f'Table stats_per_game already exists so it will not be created')
    else:
        logging.info(f'Table stats_per_game already exists will be created')
        create_table_stats_per_game(host=host, user=user, password=password, database_name=database_name)
        logging.info(f'Table stats_per_game created successfully')

    if table_exists(host=host, user=user, password=password, database_name=database_name, table_name='stats_per_minute'):
        logging.info(f'Table stats_per_minute already exists so it will not be created')
    else:
        logging.info(f'Table stats_per_minute already exists will be created')
        create_table_stats_per_minute(host=host, user=user, password=password, database_name=database_name)
        logging.info(f'Table stats_per_minute created successfully')

    if table_exists(host=host, user=user, password=password, database_name=database_name, table_name='stats_per_poss'):
        logging.info(f'Table stats_per_poss already exists so it will not be created')
    else:
        logging.info(f'Table stats_per_poss already exists will be created')
        create_table_stats_per_poss(host=host, user=user, password=password, database_name=database_name)
        logging.info(f'Table stats_per_poss created successfully')

    if table_exists(host=host, user=user, password=password, database_name=database_name, table_name='stats_totals'):
        logging.info(f'Table stats_totals already exists so it will not be created')
    else:
        logging.info(f'Table stats_totals already exists will be created')
        create_table_stats_totals(host=host, user=user, password=password, database_name=database_name)
        logging.info(f'Table stats_totals created successfully')


if __name__ == "__main__":
    build_database_with_tables(host=sql_config.HOST, user=sql_config.USER, password=sql_config.PASSWORD,
                               database_name=config.DATABASE_NAME)