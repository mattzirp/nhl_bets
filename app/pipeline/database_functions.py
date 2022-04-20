import numpy as np
import os
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.types import (String, Integer, BigInteger, Float, Date,
                              DateTime, Boolean
                              )


def get_db_uri():
    uri = os.environ['DATABASE_URL']
    return uri


def get_nst_dtype():
    nst_dtype = {'game': String(),
                 'team': String(),
                 'toi_5v5': Float(),
                 'cf_5v5': Integer(),
                 'ca_5v5': Integer(),
                 'cf%_5v5': Float(),
                 'ff_5v5': Integer(),
                 'fa_5v5': Integer(),
                 'ff%_5v5': Float(),
                 'sf_5v5': Integer(),
                 'sa_5v5': Integer(),
                 'sf%_5v5': Float(),
                 'gf_5v5': Integer(),
                 'ga_5v5': Integer(),
                 'gf%_5v5': Float(),
                 'xgf_5v5': Float(),
                 'xga_5v5': Float(),
                 'xgf%_5v5': Float(),
                 'scf_5v5': Integer(),
                 'sca_5v5': Integer(),
                 'scf%_5v5': Float(),
                 'hdcf_5v5': Integer(),
                 'hdca_5v5': Integer(),
                 'hdcf%_5v5': Float(),
                 'hdsf_5v5': Integer(),
                 'hdsa_5v5': Integer(),
                 'hdsf%_5v5': Float(),
                 'hdgf_5v5': Integer(),
                 'hdga_5v5': Integer(),
                 'hdgf%_5v5': Float(),
                 'hdsh%_5v5': Float(),
                 'hdsv%_5v5': Float(),
                 'mdcf_5v5': Integer(),
                 'mdca_5v5': Integer(),
                 'mdcf%_5v5': Float(),
                 'mdsf_5v5': Integer(),
                 'mdsa_5v5': Integer(),
                 'mdsf%_5v5': Float(),
                 'mdgf_5v5': Integer(),
                 'mdga_5v5': Integer(),
                 'mdgf%_5v5': Float(),
                 'mdsh%_5v5': Float(),
                 'mdsv%_5v5': Float(),
                 'ldcf_5v5': Integer(),
                 'ldca_5v5': Integer(),
                 'ldcf%_5v5': Float(),
                 'ldsf_5v5': Integer(),
                 'ldsa_5v5': Integer(),
                 'ldsf%_5v5': Float(),
                 'ldgf_5v5': Integer(),
                 'ldga_5v5': Integer(),
                 'ldgf%_5v5': Float(),
                 'ldsh%_5v5': Float(),
                 'ldsv%_5v5': Float(),
                 'sh%_5v5': Float(),
                 'sv%_5v5': Float(),
                 'pdo_5v5': Float(),
                 'attendance': BigInteger(),
                 'date': Date(),
                 'season': String(),
                 'team_key': String(),
                 'toi_pp': Float(),
                 'xgf_pp': Float(),
                 'gf_pp': Integer(),
                 'toi_pk': Float(),
                 'xga_pk': Float(),
                 'ga_pk': Integer(),
                 'last_game_date': Date(),
                 'b2b': Boolean()
                 }

    return nst_dtype


def get_nhl_dtype():
    nhl_dtype = {'game_id': String(),
                 'date': Date(),
                 'venue': String(),
                 'home_team': String(),
                 'away_team': String(),
                 'start_time': DateTime(),
                 'home_score': Integer(),
                 'away_score': Integer(),
                 'status': String(),
                 'home_team_won': Boolean(),
                 'home_team_key': String(),
                 'away_team_key': String()
                 }

    return nhl_dtype


def get_elo_dtype():
    elo_dtype = {'season': Integer(),
                 'date': Date(),
                 'playoff': Boolean(),
                 'neutral': Boolean(),
                 'ot': String(),
                 'home_team': String(),
                 'away_team': String(),
                 'home_team_abbr': String(),
                 'away_team_abbr': String(),
                 'home_team_pregame_rating': Float(),
                 'away_team_pregame_rating': Float(),
                 'home_team_winprob': Float(),
                 'away_team_winprob': Float(),
                 'overtime_prob': Float(),
                 'home_team_expected_points': Float(),
                 'away_team_expected_points': Float(),
                 'home_team_score': Integer(),
                 'away_team_score': Integer(),
                 'home_team_postgame_rating': Float(),
                 'away_team_postgame_rating': Float(),
                 'game_quality_rating': Float(),
                 'game_importance_rating': Float(),
                 'game_overall_rating': Float(),
                 'home_team_key': String(),
                 'away_team_key': String()
                 }

    return elo_dtype


def get_prediction_dtype():
    prediction_dtype = {'game_id': String(),
                        'date': Date(),
                        'venue': String(),
                        'home_team': String(),
                        'away_team': String(),
                        'start_time': DateTime(),
                        'home_win': Boolean(),
                        'away_prob': Float(),
                        'home_prob': Float()
                        }

    return prediction_dtype


def save_to_database(uri, df, table_name, if_exists, dtype):
    con = create_engine(uri)
    print(f'Saving to {table_name} table...')
    num_rows = df.to_sql(table_name, con, if_exists=if_exists, index=False,
                         chunksize=500, dtype=dtype
                         )
    con.dispose()

    return print(f'Saving to {table_name} complete, {num_rows} added.')


def execute_query(uri, query):
    engine = create_engine(uri)
    with engine.connect() as connection:
        connection.execute(text(query))

    return print('Query Run')


def read_query(uri, query, date_fields):
    con = create_engine(uri)
    df = pd.read_sql(sql=query,
                     con=con,
                     parse_dates=date_fields
                     )
    con.dispose()

    return df
