import argparse
import web_scraping_players_stats
import web_scraping_players_info
import build_database
import to_database_tables
import twitter_info


def caller(year_start, year_end):
    print('Starting web_scraping_players_stats')
    web_scraping_players_stats.export_players_stats(year_start, year_end)
    print('Starting web_scraping_players_info')
    web_scraping_players_info.export_players_info(year_start, year_end)
    print('Starting twitter_info')
    twitter_info.export_players_twitter_data()
    print('Starting build_database')
    build_database.build_database_with_tables()
    print('Starting to_database_tables')
    to_database_tables.write_to_tables()


def generate_data():
    my_parser = argparse.ArgumentParser()

    my_parser.add_argument('year_start', help='year of the season that we will start scraping stats from')
    my_parser.add_argument('year_end', help='year of the season that we will finish scraping stats from')

    args = my_parser.parse_args()
    caller(int(args.year_start), int(args.year_end))


if __name__ == "__main__":
    generate_data()
