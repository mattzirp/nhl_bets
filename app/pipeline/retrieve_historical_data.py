"""Retrieve Historical NHL Data

This script is intended to be run once to initialize a postgres database
with historical NHL data from the prior three seasons and the current
season up until the current date. Data is acquired from NaturalStatTrick
(web scraping), the NHL API (hockey_scraper), and fivethirtyeight ELO
rankings (scraped .csv drop). The retrieved data is then saved to a
postgres database.

The script will create the following tables on the nhl_bets database:
- nst_team_stats
- team_elo
- game_results
"""

from datetime import date, timedelta
import ssl
from scraping_functions import (get_season_string,
                                nst_pipeline,
                                nhl_pipeline,
                                elo_pipeline,
                                )
from database_functions import (get_db_uri,
                                get_nst_dtype,
                                get_nhl_dtype,
                                get_elo_dtype,
                                save_to_database
                                )

TODAY = date.today()
ssl._create_default_https_context = ssl._create_unverified_context
SQLALCHEMY_DATABASE_URI = get_db_uri()


NST_DTYPE = get_nst_dtype()
NHL_DTYPE = get_nhl_dtype()
ELO_DTYPE = get_elo_dtype()


if __name__ == '__main__':
    seasons = []
    for i in range(4):
        season = get_season_string(TODAY - timedelta(weeks=i*52))
        seasons.append(season)
        seasons.sort()
    for season in seasons:

        nst_data = nst_pipeline(from_season=season,
                                to_season=season
                                )
        save_to_database(uri=SQLALCHEMY_DATABASE_URI,
                         df=nst_data,
                         table_name='nst',
                         if_exists='append',
                         dtype=NST_DTYPE)

    start_date = seasons[0][:4] + '-07-01'
    yesterday = TODAY - timedelta(days=1)
    end_date = yesterday.strftime('%Y-%m-%d')
    nhl_data = nhl_pipeline(start_date=start_date, end_date=end_date)
    save_to_database(uri=SQLALCHEMY_DATABASE_URI,
                     df=nhl_data,
                     table_name='nhl',
                     if_exists='replace',
                     dtype=NHL_DTYPE)

    elo_data = elo_pipeline(start_date=start_date, end_date=end_date)
    save_to_database(uri=SQLALCHEMY_DATABASE_URI,
                     df=elo_data,
                     table_name='elo',
                     if_exists='replace',
                     dtype=ELO_DTYPE)
