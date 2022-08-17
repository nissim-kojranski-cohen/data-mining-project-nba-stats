import pvt_data_config
import csv
import pymysql
import os
import pathlib
import config
import logging

logging.basicConfig(filename='nba_web_scrapping.log', encoding='utf-8', level=logging.INFO, format=config.LOG_FORMAT)


def execute_query(query, executemany=False, tup_list=None):
    """
    :param query: query to execute
    :param executemany: if True execute statement of multiple queries
    :param tup_list: tuples to execute if executemany is True
    :return: query result for single query
    """
    connection = pymysql.connect(host=pvt_data_config.HOST,
                                 user=pvt_data_config.USER,
                                 password=pvt_data_config.PASSWORD,
                                 database=config.DATABASE_NAME)
    with connection:
        with connection.cursor() as cursor:
            sql = query
            # execute one query
            if not executemany:
                cursor.execute(sql)
                result = cursor.fetchall()
                return result
            # execute many queries
            else:
                cursor.executemany(sql, tup_list)
                connection.commit()


def generate_row(row_data, varchar_cols=[], omited_cols=[]):
    """
    :param row_data: dictionary with the data from one row of the csv file
    :param varchar_cols: cols of type varchar
    :param omited_cols: cols that will not be saved
    :return: a list with the data of one row in the csv file
    """
    row_data_list = []
    # creating a list for each row by its value type
    for key in row_data:
        if key in omited_cols:
            pass
        elif key in varchar_cols:
            row_data_list.append(row_data[key])
        elif row_data[key] == '' or row_data[key] == 'Undrafted':
            row_data_list.append(None)
        else:
            row_data_list.append(float(row_data[key]))
    return row_data_list


def get_db_rows(table_name, col_names):
    """
    returns a result of sql query
    :param table_name: database table name
    :param col_names: table specific columns
    :return: tuple of the identifing columns of the table
    """
    in_db = execute_query(f"""SELECT {', '.join(col_names)} FROM {table_name}""")
    return in_db


def insert_tuple_to_db(tup_list, table_name):
    """
    takes a list of tuples to insert into database table
    :param tup_list: data to be inserted to table
    :param table_name: table that the data is inserted to
    """
    # check if there is new data to insert
    if len(tup_list) > 0:
        col_num = '%s, ' * len(tup_list[0])
        stmt = f"INSERT INTO {table_name} VALUES ({col_num[:-2]})"
        execute_query(stmt, executemany=True, tup_list=tup_list)
        logging.info(f'Data inserted to {table_name} table')
    else:
        logging.info(f'No new data to insert to {table_name} table')


def to_stats_table(filename):
    """
    takes csv file and insert it to the appropriate mysql table in nba_data db
    :param filename: string representing the file name
    :return: None
    """
    with open(filename, encoding='utf-8') as file:
        # extract year and stat type from file name
        year = int(filename.split('_')[1])
        type_of_stat = '_'.join(filename.split('_')[2:]).split('.')[0]
        logging.info(f'finshed reading {filename}')

        # read the csv
        reader = csv.DictReader(file)
        # store each dictionary (representing a player) to tupel list
        tup_list = []
        # query to check if sample is in database
        in_db = get_db_rows(table_name=f'stats_{type_of_stat}', col_names=['player_id', 'season'])
        varchar_cols = ['player_id', 'pos', 'team_id']
        omited_cols = ['', 'player', 'age']
        for index, player_data in enumerate(reader):
            # creating a list for each row by its value type
            row_data = generate_row(player_data, varchar_cols=varchar_cols, omited_cols=omited_cols)
            # insert year column
            row_data.insert(3, year)
            # checking if sample is in database, only new data is stored
            if (row_data[0], row_data[3]) in in_db:
                # the sample exists in the database
                pass
            else:
                # saving the row data to tuple and storing in tup_list
                tup_list.append(tuple(row_data))

        insert_tuple_to_db(tup_list, table_name=f'stats_{type_of_stat}')


def to_players_table(filename):
    """
    takes csv file and insert it to the appropriate mysql table in nba_data db
    :param filename: string representing the file name
    :return: None
    """
    with open(filename, encoding='utf-8') as file:
        # extract year and stat type from file name
        table_name = filename.split('.')[0]
        if table_name == 'players_id':
            table_name = 'players'
        logging.info(f'finshed reading {filename}')

        # read the csv
        reader = csv.DictReader(file)
        # store each dictionary (representing a player) to tuple list
        tup_list = []
        # query to check if sample is in database
        in_db = get_db_rows(table_name=table_name, col_names=['player_id'])
        varchar_cols = ['player_id', 'team', 'college', 'country', 'player']
        omited_cols = ['']
        for index, player_data in enumerate(reader):
            # creating a list for each row by its value type
            row_data = generate_row(player_data, varchar_cols=varchar_cols, omited_cols=omited_cols)

            # checking if sample is in database, only new data is stored
            if (row_data[0],) in in_db:
                pass
            else:
                # saving the row data to tuple and storing in tup_list
                tup_list.append(tuple(row_data))

        insert_tuple_to_db(tup_list, table_name=table_name)


def to_twitter_table(filename):
    """
    takes csv file and insert it to the appropriate mysql table in nba_data db
    :param filename: string representing the file name
    """
    with open(filename, encoding='utf-8') as file:
        # extract year and stat type from file name
        table_name = filename.split('.')[0]
        logging.info(f'finshed reading {filename}')

        # read the csv
        reader = csv.DictReader(file)
        # store each dictionary (representing a player) to tuple list
        tup_list = []
        # query to check if sample is in database
        in_db = get_db_rows(table_name=table_name, col_names=['player_id'])
        varchar_cols = ['player_id', 'creation_date', 'user_name', 'description']
        omited_cols = ['']
        for index, player_data in enumerate(reader):
            # creating a list for each row by its value type
            row_data = generate_row(player_data, varchar_cols=varchar_cols, omited_cols=omited_cols)

            # checking if sample is in database, only new data is stored
            if (row_data[0]) in in_db:
                pass
            else:
                # saving the row data to tuple and storing in tup_list
                tup_list.append(tuple(row_data))

        insert_tuple_to_db(tup_list, table_name=table_name)


def to_teams_table():
    """ inserts data to teams table """
    teams_tup_list = [('ATL', 'Atlanta Hawks'),
                      ('BOS', 'Boston Celtics'),
                      ('BRK', 'Brooklyn Nets'),
                      ('CHI', 'Charlotte Hornets'),
                      ('CHO', 'Chicago Bulls'),
                      ('CLE', 'Cleveland Cavaliers'),
                      ('DAL', 'Dallas Mavericks'),
                      ('DEN', 'Denver Nuggets'),
                      ('DET', 'Detroit Pistons'),
                      ('GSW', 'Golden State Warriors'),
                      ('HOU', 'Houston Rockets'),
                      ('IND', 'Indiana Pacers'),
                      ('LAC', 'LA Clippers'),
                      ('LAL', 'Los Angeles Lakers'),
                      ('MEM', 'Memphis Grizzlies'),
                      ('MIA', 'Miami Heat'),
                      ('MIL', 'Milwaukee Bucks'),
                      ('MIN', 'Minnesota Timberwolves'),
                      ('NOP', 'New Orleans Pelicans'),
                      ('NYK', 'New York Knicks'),
                      ('OKC', 'Oklahoma City Thunder'),
                      ('ORL', 'Orlando Magic'),
                      ('PHI', 'Philadelphia 76ers'),
                      ('PHO', 'Phoenix Suns'),
                      ('POR', 'Portland Trail Blazers'),
                      ('SAC', 'Sacramento Kings'),
                      ('SAS', 'San Antonio Spurs'),
                      ('TOR', 'Toronto Raptors'),
                      ('TOT', 'Total'),
                      ('UTA', 'Utah Jazz'),
                      ('WAS', 'Washington Wizards')]

    col_num = '%s, ' * len(teams_tup_list[0])
    stmt = f"INSERT INTO teams VALUES ({col_num[:-2]})"
    execute_query(stmt, executemany=True, tup_list=teams_tup_list)
    logging.info(f'Data inserted to teams table')


def write_file_types(file_list, db_func, startswith, endswith='.csv'):
    """
    :param file_list: list of files to process
    :param db_func: function to save data to specific table
    :param startswith: characters in the start of file name
    :param endswith: characters in the end of file name
    """
    for file in file_list:
        # insert specific type of csv files to mysql tables
        if file.endswith(endswith) and file.startswith(startswith):
            db_func(file)
            logging.info(f'Data for {file} inserted succesfully')


def write_to_tables():
    current_path = pathlib.Path().resolve()
    # list of all files in current directory
    onlyfiles = [f for f in os.listdir(current_path) if os.path.isfile(os.path.join(current_path, f))]

    # insert csv files data to mysql database, for each table type
    to_teams_table()
    write_file_types(onlyfiles, db_func=to_players_table, startswith='players', endswith='.csv')
    write_file_types(onlyfiles, db_func=to_stats_table, startswith='sample', endswith='.csv')
    write_file_types(onlyfiles, db_func=to_twitter_table, startswith='twitter_details', endswith='.csv')
    

if __name__ == "__main__":
    write_to_tables()
