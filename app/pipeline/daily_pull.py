"""Daily Pull - Scrape NHL data

This script is intended to run daily to retrieve new game data from the prior
day's games, as well as today's upcoming games to be fed to the prediction
model
"""

from datetime import date, timedelta
import ssl
from scraping_functions import (get_season_string,
                                nst_pipeline,
                                nst_filter_date,
                                nhl_pipeline,
                                elo_pipeline,
                                )
from database_functions import (get_db_uri,
                                get_nst_dtype,
                                get_nhl_dtype,
                                get_elo_dtype,
                                save_to_database,
                                execute_query
                                )
import build_team_stats_table
import build_features

ssl._create_default_https_context = ssl._create_unverified_context

TODAY = date.today()
SQLALCHEMY_DATABASE_URI = get_db_uri()
NST_DTYPE = get_nst_dtype()
NHL_DTYPE = get_nhl_dtype()
ELO_DTYPE = get_elo_dtype()


if __name__ == '__main__':
    this_season = get_season_string(TODAY)
    yesterday = TODAY - timedelta(days=1)

    nst_data = nst_pipeline(from_season=this_season, to_season=this_season)
    nst_data = nst_filter_date(nst_data, yesterday)
    save_to_database(uri=SQLALCHEMY_DATABASE_URI,
                     df=nst_data,
                     table_name='nst',
                     if_exists='append',
                     dtype=NST_DTYPE)

    target_date = yesterday.strftime('%Y-%m-%d')
    nhl_data = nhl_pipeline(start_date=target_date, end_date=target_date)
    save_to_database(uri=SQLALCHEMY_DATABASE_URI,
                     df=nhl_data,
                     table_name='nhl',
                     if_exists='append',
                     dtype=NHL_DTYPE)

    elo_data = elo_pipeline(start_date=target_date, end_date=target_date)
    save_to_database(uri=SQLALCHEMY_DATABASE_URI,
                     df=elo_data,
                     table_name='elo',
                     if_exists='append',
                     dtype=ELO_DTYPE)

    todays_games = nhl_pipeline(start_date=TODAY.strftime('%Y-%m-%d'),
                                end_date=TODAY.strftime('%Y-%m-%d')
                                )
    save_to_database(uri=SQLALCHEMY_DATABASE_URI,
                     df=todays_games,
                     table_name='todays_games',
                     if_exists='replace',
                     dtype=NHL_DTYPE
                     )

    execute_query(uri=SQLALCHEMY_DATABASE_URI,
                  query=build_team_stats_table.query
                  )

    execute_query(uri=SQLALCHEMY_DATABASE_URI,
                  query=build_features.query
                  )
