from datetime import date
from scraping_functions import nhl_pipeline
from database_functions import get_db_uri, get_nhl_dtype, save_to_database, execute_query
import build_team_stats_table
import build_features

TODAY = date.today()
SQLALCHEMY_DATABASE_URI = get_db_uri()
NHL_DTYPE = get_nhl_dtype()


if __name__ == '__main__':

    target_date = TODAY.strftime('%Y-%m-%d')
    todays_games = nhl_pipeline(start_date=target_date, end_date=target_date)
    save_to_database(uri=SQLALCHEMY_DATABASE_URI,
                     df=todays_games,
                     table_name='todays_games',
                     if_exists='replace',
                     dtype=NHL_DTYPE
                     )

    execute_query(uri=SQLALCHEMY_DATABASE_URI,
                  query=build_team_stats_table.query
                  )
    execute_query(uri=SQLALCHEMY_DATABASE_URI, query=build_features.query)
