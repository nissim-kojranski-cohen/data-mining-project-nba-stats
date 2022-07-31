import pymysql
import config
import sql_config


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


def build_database_with_tables(host, user, password, database_name):
    """
    Executes all commands to build the database schema for nba_data
    Created tables names: 'players', 'players_info', 'teams', 'stats_per_game', 'stats_per_minute',
    'stats_per_poss', 'stats_totals'
    :param host: name of the host
    :param user: name of the user
    :param password: MySQL password
    :param database_name: name of the database
    """
    create_database(host=host, user=user, password=password, database_name=database_name)
    create_table_players(host=host, user=user, password=password, database_name=database_name)
    create_table_players_info(host=host, user=user, password=password, database_name=database_name)
    create_table_teams(host=host, user=user, password=password, database_name=database_name)
    create_table_stats_per_game(host=host, user=user, password=password, database_name=database_name)
    create_table_stats_per_minute(host=host, user=user, password=password, database_name=database_name)
    create_table_stats_per_poss(host=host, user=user, password=password, database_name=database_name)
    create_table_stats_totals(host=host, user=user, password=password, database_name=database_name)


if __name__ == "__main__":
    build_database_with_tables(host=sql_config.HOST, user=sql_config.USER, password=sql_config.PASSWORD,
                               database_name=config.DATABASE_NAME)