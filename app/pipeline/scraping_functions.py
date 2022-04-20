"""Scraping functions

"""

from datetime import date
import numpy as np
import pandas as pd
import hockey_scraper


TODAY = date.today()


def get_season_string(date):
    """Determines the season string for given date

    Parameters
    ----------
    date - A datetime

    Returns
    -------
    8 digit season string, ex '20212022'
    """

    current_year = date.year

    if date.month > 7:
        # early season, current year is first year of season
        season_string = str(current_year) + str(current_year+1)

    elif date.month <= 7:
        # late season, current year is second year of season
        season_string = str(current_year-1) + str(current_year)

    return season_string


def nst_scrape_games(from_season, to_season, sit='5v5'):
    """Scrapes naturalstattrick.com (nst) games page to retrieve team stats for
    each game played

    Parameters
    ----------
    from_season - year to start an 8 char str of an NHL season (ex. '20202021')
    to_season - year to end an 8 char str of an NHL season (ex.20212022)
    sit - on ice situation, one of [5v5, pp, pk]
    rate - return counts or rates per 60min of play, (y/n)

    Returns
    -------
    pandas DataFrame containing team stats for each game played"""
    # url for NaturalStatTrick games table
    url = f'https://www.naturalstattrick.com/games.php?fromseason={from_season}&thruseason={to_season}&stype=2&sit={sit}&loc=B&team=All&rate=n' # noqa

    # scrape html table from webpage
    df = pd.read_html(url, header=0, index_col=0, na_values=["-"])[0]

    # reset index
    df.reset_index(inplace=True)

    # drop links column
    df = df.drop(columns='Unnamed: 2')

    # format date column
    df['date'] = df['Game'].apply(lambda x: pd.to_datetime(x[:10],
                                                           format='%Y-%m-%d'))
    df['season'] = df['date'].apply(lambda x: get_season_string(x))

    return df


def nst_add_game_number(df):
    """Adds a game_number column to nst scraped games data which giving
    which number game of the season it is for a given team

    Parameters
    ----------
    pandas DataFrame containing
    """

    # add team game number column
    df['game_number'] = df.groupby(['season', 'Team']).cumcount() + 1

    return df


def nst_replace_names(df):
    # dictionary to convert team names to NHL official char abbrev
    nst_teamname_conversion = {
                                'Anaheim Ducks': 'ANA',
                                'Arizona Coyotes': 'ARI',
                                'Boston Bruins': 'BOS',
                                'Buffalo Sabres': 'BUF',
                                'Calgary Flames': 'CGY',
                                'Carolina Hurricanes': 'CAR',
                                'Chicago Blackhawks': 'CHI',
                                'Colorado Avalanche': 'COL',
                                'Columbus Blue Jackets': 'CBJ',
                                'Dallas Stars': 'DAL',
                                'Detroit Red Wings': 'DET',
                                'Edmonton Oilers': 'EDM',
                                'Florida Panthers': 'FLA',
                                'Los Angeles Kings': 'L.A',
                                'Minnesota Wild': 'MIN',
                                'Montreal Canadiens': 'MTL',
                                'Nashville Predators': 'NSH',
                                'New Jersey Devils': 'N.J',
                                'New York Islanders': 'NYI',
                                'New York Rangers': 'NYR',
                                'Ottawa Senators': 'OTT',
                                'Philadelphia Flyers': 'PHI',
                                'Pittsburgh Penguins': 'PIT',
                                'San Jose Sharks': 'S.J',
                                'Seattle Kraken': 'SEA',
                                'St Louis Blues': 'STL',
                                'Tampa Bay Lightning': 'T.B',
                                'Toronto Maple Leafs': 'TOR',
                                'Vancouver Canucks': 'VAN',
                                'Vegas Golden Knights': 'VGK',
                                'Washington Capitals': 'WSH',
                                'Winnipeg Jets': 'WPG'
    }

    # convert team name to abbreviations
    df = df.replace({'Team': nst_teamname_conversion})

    return df


def nst_add_key(df):
    # create key for merging data
    df['team_key'] = df['Team'].astype(str) + '_' + df['date'].astype(str)

    return df


def nst_single_sit(from_season, to_season, sit='5v5'):
    df = nst_scrape_games(from_season, to_season)
    df = nst_replace_names(df)
    df = nst_add_key(df)

    if sit == '5v5':
        df = nst_add_game_number(df)

    return df


def nst_get_merge_sits(from_season, to_season):
    sits = ['5v5', 'pp', 'pk']
    sit_dfs = []

    for sit in sits:
        df = nst_single_sit(from_season, to_season, sit=sit)
        sit_dfs.append(df)

    df = sit_dfs[0]
    df = df.merge(sit_dfs[1][['team_key', 'TOI', 'xGF', 'GF']],
                  on='team_key',
                  how='left',
                  suffixes=('', '_'+sits[1])
                  )

    df = df.merge(sit_dfs[2][['team_key', 'TOI', 'xGA', 'GA']],
                  on='team_key',
                  how='left',
                  suffixes=('', '_'+sits[2])
                  )
    return df


def nst_clean_special_teams(df):

    # account for games with no special teams TOI
    df['TOI_pp'] = np.where(df['TOI_pp'].isna(), 0, df['TOI_pp'])
    df['TOI_pk'] = np.where(df['TOI_pk'].isna(), 0, df['TOI_pk'])
    df['xGF_pp'] = np.where(df['xGF_pp'].isna(), 0, df['xGF_pp'])
    df['GF_pp'] = np.where(df['GF_pp'].isna(), 0, df['GF_pp'])
    df['xGA_pk'] = np.where(df['xGA_pk'].isna(), 0, df['xGA_pk'])
    df['GA_pk'] = np.where(df['GA_pk'].isna(), 0, df['GA_pk'])

    return df


def nst_add_b2b(df):
    df['last_game_date'] = df.groupby('Team')['date'].shift()
    df['days_since_last_game'] = df['date'] - df['last_game_date']
    df['b2b'] = np.where(df['days_since_last_game'] == '1 days', 1, 0)
    df = df.drop('days_since_last_game', axis='columns')

    return df


def nst_format(df):
    mapper = {'Game': 'game',
              'Team': 'team',
              'TOI': 'toi_5v5',
              'CF': 'cf_5v5',
              'CA': 'ca_5v5',
              'CF%': 'cf%_5v5',
              'FF': 'ff_5v5',
              'FA': 'fa_5v5',
              'FF%': 'ff%_5v5',
              'SF': 'sf_5v5',
              'SA': 'sa_5v5',
              'SF%': 'sf%_5v5',
              'GF': 'gf_5v5',
              'GA': 'ga_5v5',
              'GF%': 'gf%_5v5',
              'xGF': 'xgf_5v5',
              'xGA': 'xga_5v5',
              'xGF%': 'xgf%_5v5',
              'SCF': 'scf_5v5',
              'SCA': 'sca_5v5',
              'SCF%': 'scf%_5v5',
              'HDCF': 'hdcf_5v5',
              'HDCA': 'hdca_5v5',
              'HDCF%': 'hdcf%_5v5',
              'HDSF': 'hdsf_5v5',
              'HDSA': 'hdsa_5v5',
              'HDSF%': 'hdsf%_5v5',
              'HDGF': 'hdgf_5v5',
              'HDGA': 'hdga_5v5',
              'HDGF%': 'hdgf%_5v5',
              'HDSH%': 'hdsh%_5v5',
              'HDSV%': 'hdsv%_5v5',
              'MDCF': 'mdcf_5v5',
              'MDCA': 'mdca_5v5',
              'MDCF%': 'mdcf%_5v5',
              'MDSF': 'mdsf_5v5',
              'MDSA': 'mdsa_5v5',
              'MDSF%': 'mdsf%_5v5',
              'MDGF': 'mdgf_5v5',
              'MDGA': 'mdga_5v5',
              'MDGF%': 'mdgf%_5v5',
              'MDSH%': 'mdsh%_5v5',
              'MDSV%': 'mdsv%_5v5',
              'LDCF': 'ldcf_5v5',
              'LDCA': 'ldca_5v5',
              'LDCF%': 'ldcf%_5v5',
              'LDSF': 'ldsf_5v5',
              'LDSA': 'ldsa_5v5',
              'LDSF%': 'ldsf%_5v5',
              'LDGF': 'ldgf_5v5',
              'LDGA': 'ldga_5v5',
              'LDGF%': 'ldgf%_5v5',
              'LDSH%': 'ldsh%_5v5',
              'LDSV%': 'ldsv%_5v5',
              'SH%': 'sh%_5v5',
              'SV%': 'sv%_5v5',
              'PDO': 'pdo_5v5',
              'Attendance': 'attendance',
              'TOI_pp': 'toi_pp',
              'xGF_pp': 'xgf_pp',
              'GF_pp': 'gf_pp',
              'TOI_pk': 'toi_pk',
              'xGA_pk': 'xga_pk',
              'GA_pk': 'ga_pk'
              }

    df = df.rename(mapper=mapper, axis='columns')

    return df


def nst_pipeline(from_season, to_season):
    df = nst_get_merge_sits(from_season, to_season)
    df = nst_clean_special_teams(df)
    df = nst_add_b2b(df)
    df = nst_format(df)

    return df


def nst_filter_date(df, date):
    df = df[(df['date'] == date)]
    return df


def nhl_scrape_games(start_date, end_date):
    # scrape game data between two dates, get in dataframe format
    df = hockey_scraper.scrape_schedule(start_date, end_date,
                                        data_format='Pandas'
                                        )

    return df


def nhl_filter_games(df):
    # map game_id to str, use to filter out pre/post season games
    df['game_id'] = df['game_id'].map(str)

    # middle two numbers of the game_id string are
    # 01 - preseason, 02 - regular season, 03 - playoffs
    mask = ((df['game_id'].str[4:6] != '01') &
            (df['game_id'].str[4:6] != '03')
            )
    df = df[mask]

    return df


def nhl_add_home_win(df):
    # create categorical column for home team win
    df['home_team_won'] = np.where(df['home_score'] > df['away_score'], 1, 0)

    return df


def nhl_add_key(df):
    # create keys for home and away team for merging data
    df['home_team_key'] = (df['home_team'].astype(str) + '_'
                           + df['date'].astype(str)
                           )

    df['away_team_key'] = (df['away_team'].astype(str) + '_'
                           + df['date'].astype(str)
                           )

    return df


def nhl_pipeline(start_date, end_date):
    df = nhl_scrape_games(start_date, end_date)
    df = nhl_filter_games(df)
    df = nhl_add_home_win(df)
    df = nhl_add_key(df)

    return df


def elo_scrape():
    url = 'https://projects.fivethirtyeight.com/nhl-api/nhl_elo_latest.csv'
    # link to historical ELO ratings
    hist_url = 'https://projects.fivethirtyeight.com/nhl-api/nhl_elo.csv'
    df = pd.read_csv(url)
    df2 = pd.read_csv(hist_url)
    df = pd.concat(objs=[df, df2])

    return df


def elo_filter(df, start_date, end_date):
    # filter to target date range
    df = df[((df['date'] > start_date) & (df['date'] < end_date))]
    # filter out playoff games
    df = df[(df['playoff'] != '1')]

    return df


def elo_replace_abbr(df):
    # dictionary to convert fivethirtyeight ELO team abbreviations to
    # NHL official 3 character abbreviations
    elo_abbrev_conversion = {
                                'LAK': 'L.A',
                                'NJD': 'N.J',
                                'SJS': 'S.J',
                                'TBL': 'T.B',
                                'VEG': 'VGK'
    }

    # replace inconsistent abbreviations
    df = df.replace({'home_team_abbr': elo_abbrev_conversion})
    df = df.replace({'away_team_abbr': elo_abbrev_conversion})

    return df


def elo_add_key(df):
    # add team key
    df['home_team_key'] = (df['home_team_abbr'].astype(str) + '_'
                           + df['date'].astype(str)
                           )
    df['away_team_key'] = (df['away_team_abbr'].astype(str) + '_'
                           + df['date'].astype(str)
                           )

    return df


def elo_pipeline(start_date, end_date):
    df = elo_scrape()
    df = elo_filter(df, start_date, end_date)
    df = elo_replace_abbr(df)
    df = elo_add_key(df)

    return df
