import sql_config
import csv
import pymysql
import os
import pathlib


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

        # read the csv
        reader = csv.DictReader(file)
        # store each dictionary (representing a player) to tupel list
        tup_list = []
        for index, player_data in enumerate(reader):
            varchar_cols = ['player_id', 'pos', 'team_id']
            omited_cols = ['player', 'age']
            # creating a list for each row by its value type
            row_data = []
            for key in player_data:
                if key in omited_cols:
                    pass
                elif key in varchar_cols:
                    row_data.append(player_data[key])
                elif player_data[key] == '':
                    row_data.append(None)
                else:
                    row_data.append(float(player_data[key]))
            # insert year column
            row_data.insert(3, year)
            # saving the row data to tuple and storing in tup_list
            tup_list.append(tuple(row_data))

        # connecting to mysql server
        connection = pymysql.connect(host=sql_config.HOST,
                                     user=sql_config.USER,
                                     password=sql_config.PASSWORD,
                                     database=sql_config.DATABASE_NAME)

        # inserting the data to mysql table
        with connection:
            with connection.cursor() as cursor:
                col_num = '%s, ' * len(tup_list[0])
                stmt = f"INSERT INTO stats_{type_of_stat} VALUES ({col_num[:-2]})"
                cursor.executemany(stmt, tup_list)
                connection.commit()


def main():
    current_path = pathlib.Path().resolve()
    # list of all files in current directory
    onlyfiles = [f for f in os.listdir(current_path) if os.path.isfile(os.path.join(current_path, f))]
    for file in onlyfiles:
        # insert all stats csv files to mysql tables
        if file.endswith('.csv') and file.startswith('sample'):
            to_stats_table(file)


if __name__ == "__main__":
    main()