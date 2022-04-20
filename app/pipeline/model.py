import pandas as pd
from database_functions import (get_db_uri, get_prediction_dtype,
                                read_query, save_to_database)
import select_features
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.compose import ColumnTransformer
from sklearn.linear_model import LogisticRegression
import select_todays_games
import select_most_recent_stats

SQLALCHEMY_DATABASE_URI = get_db_uri()
PREDICTION_DTYPE = get_prediction_dtype()


def build_prediction_df(todays_games, recent_stats):
    df = todays_games.merge(recent_stats.add_prefix('home_'), on='home_team')
    df = df.merge(recent_stats.add_prefix('away_'), on='away_team')
    df = df.drop(columns=['home_score',
                          'away_score',
                          'status',
                          'home_team_won',
                          'home_team_key_y',
                          'home_date',
                          'away_team_key_y',
                          'away_date'])

    mapper = {'home_team_key_x': 'home_team_key',
              'away_team_key_x': 'away_team_key',
              'home_ff%_5v5_last_half': 'home_ff_last_half',
              'home_gf%_5v5_last_half': 'home_gf_last_half',
              'home_xgf%_5v5_last_half': 'home_xgf_last_half',
              'home_sh%_5v5_last_half': 'home_sh_last_half',
              'home_gf_per_min_pp_last_half': 'home_gf_min_pp',
              'home_xgf_per_min_pp_last_half': 'home_xgf_min_pp',
              'home_ga_per_min_pk_last_half': 'home_ga_min_pk',
              'home_xga_per_min_pk_last_half': 'home_xga_min_pk',
              'away_ff%_5v5_last_half': 'away_ff_last_half',
              'away_gf%_5v5_last_half': 'away_gf_last_half',
              'away_xgf%_5v5_last_half': 'away_xgf_last_half',
              'away_sh%_5v5_last_half': 'away_sh_last_half',
              'away_gf_per_min_pp_last_half': 'away_gf_min_pp',
              'away_xgf_per_min_pp_last_half': 'away_xgf_min_pp',
              'away_ga_per_min_pk_last_half': 'away_ga_min_pk',
              'away_xga_per_min_pk_last_half': 'away_xga_min_pk'
              }

    df = df.rename(mapper=mapper, axis='columns')

    return df


if __name__ == '__main__':
    features = read_query(uri=SQLALCHEMY_DATABASE_URI,
                          query=select_features.query,
                          date_fields={'evaluated': '%Y-%m-%d'}
                          )

    numeric_features = ['home_ff_last_half',
                        'home_gf_last_half',
                        'home_xgf_last_half',
                        'home_sh_last_half',
                        'home_gf_min_pp',
                        'home_xgf_min_pp',
                        'home_ga_min_pk',
                        'home_xga_min_pk',
                        'away_ff_last_half',
                        'away_gf_last_half',
                        'away_xgf_last_half',
                        'away_sh_last_half',
                        'away_gf_min_pp',
                        'away_xgf_min_pp',
                        'away_ga_min_pk',
                        'away_xga_min_pk',
                        ]

    categorical_features = ['home_b2b',
                            'away_b2b'
                            ]

    all_features = numeric_features + categorical_features
    X = features.loc[:, all_features]
    y = features.loc[:, 'home_team_won']

    numeric_transformer = Pipeline(steps=[('scaler', StandardScaler())])
    categorical_transformer = Pipeline(steps=[('ohe', OneHotEncoder())])
    preprocessor = ColumnTransformer(transformers=[
                        ('num', numeric_transformer, numeric_features),
                        ('cat', categorical_transformer, categorical_features)
                        ])

    logreg = LogisticRegression(penalty='l2', C=0.01, solver='liblinear')
    logreg_pipeline = Pipeline(steps=[
                    ('preprocessor', preprocessor),
                    ('logisticregression', logreg)
                    ])

    logreg_pipeline.fit(X, y)

    todays_games = read_query(uri=SQLALCHEMY_DATABASE_URI,
                              query=select_todays_games.query,
                              date_fields={'date': '%Y-%m-%d'}
                              )
    recent_stats = read_query(uri=SQLALCHEMY_DATABASE_URI,
                              query=select_most_recent_stats.query,
                              date_fields={'date': '%Y-%m-%d'}
                              )

    X_pred = build_prediction_df(todays_games, recent_stats)
    X_pred = X_pred.loc[:, all_features]
    y_pred = logreg_pipeline.predict(X_pred)
    y_prob = logreg_pipeline.predict_proba(X_pred)

    prediction_df = pd.concat([todays_games,
                               pd.DataFrame(y_pred, columns=['home_win']),
                               pd.DataFrame(y_prob, columns=['away_prob',
                                                             'home_prob'])],
                              axis='columns'
                              )

    prediction_df = prediction_df.drop(columns=['home_score',
                                                'away_score',
                                                'status',
                                                'home_team_won',
                                                'home_team_key',
                                                'away_team_key'])

    prediction_df['home_prob'] = prediction_df['home_prob'].map('{:,.3f}'.format)
    prediction_df['away_prob'] = prediction_df['away_prob'].map('{:,.3f}'.format)

    save_to_database(uri=SQLALCHEMY_DATABASE_URI,
                     df=prediction_df,
                     table_name='predictions',
                     if_exists='append',
                     dtype=PREDICTION_DTYPE
                     )
