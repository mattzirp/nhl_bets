query = """
CREATE TABLE IF NOT EXISTS features(
	game_id varchar,
	home_team_won boolean,
	home_team varchar,
	home_team_key varchar,
	home_ff_last_half double precision,
	home_gf_last_half double precision, 
	home_xgf_last_half double precision,
	home_sh_last_half double precision,
	home_gf_min_pp double precision,
	home_xgf_min_pp double precision,
	home_ga_min_pk double precision,
	home_xga_min_pk double precision,
	home_b2b boolean,
	away_team varchar,
	away_team_key varchar,
	away_ff_last_half double precision,
	away_gf_last_half double precision, 
	away_xgf_last_half double precision,
	away_sh_last_half double precision,
	away_gf_min_pp double precision,
	away_xgf_min_pp double precision,
	away_ga_min_pk double precision,
	away_xga_min_pk double precision,
	away_b2b boolean,
	home_elo double precision,
	away_elo double precision,
	evaluated date,
	PRIMARY KEY (game_id)
);

INSERT INTO 
	features(
		game_id, 
		home_team_won,
		home_team,
		home_team_key,
		home_ff_last_half,
		home_gf_last_half,
		home_xgf_last_half,
		home_sh_last_half,
		home_gf_min_pp,
		home_xgf_min_pp,
		home_ga_min_pk,
		home_xga_min_pk,
		home_b2b,
		away_team,
		away_team_key,
		away_ff_last_half,
		away_gf_last_half,
		away_xgf_last_half,
		away_sh_last_half,
		away_gf_min_pp,
		away_xgf_min_pp,
		away_ga_min_pk,
		away_xga_min_pk,
		away_b2b,
		home_elo,
		away_elo,
		evaluated
	)
SELECT
	DISTINCT(game_id),
	home_team_won, 
	nhl.home_team,
	nhl.home_team_key, 
	hts."ff%_5v5_last_half" as "home_ff_last_half",
	hts."gf%_5v5_last_half" as "home_gf_last_half",
	hts."xgf%_5v5_last_half" as "home_xgf_last_half",
	hts."sh%_5v5_last_half" as "home_sh_last_half",
	hts.gf_per_min_pp_last_half as "home_gf_min_pp",
	hts.xgf_per_min_pp_last_half as "home_xgf_min_pp",
	hts.ga_per_min_pk_last_half as "home_ga_min_pk",
	hts.xga_per_min_pk_last_half as "home_xga_min_pk",
	hts.b2b as home_b2b,
	nhl.away_team,
	nhl.away_team_key,
	ats."ff%_5v5_last_half" as "away_ff_last_half",
	ats."gf%_5v5_last_half" as "away_gf_last_half",
	ats."xgf%_5v5_last_half" as "away_xgf_last_half",
	ats."sh%_5v5_last_half" as "away_sh_last_half",
	ats."gf_per_min_pp_last_half" as "away_gf_min_pp",
	ats."xgf_per_min_pp_last_half" as "away_xgf_min_pp",
	ats."ga_per_min_pk_last_half" as "away_ga_min_pk",
	ats."xga_per_min_pk_last_half" as "away_xga_min_pk",
	ats."b2b" as away_b2b,
	elo.home_team_pregame_rating as home_elo,
	elo.away_team_pregame_rating as away_elo,
	CURRENT_DATE AS "evaluated"
FROM nhl
	LEFT JOIN team_stats AS hts ON nhl.home_team_key = hts.team_key
	LEFT JOIN team_stats AS ats ON nhl.away_team_key = ats.team_key
	INNER JOIN elo ON nhl.home_team_key = elo.home_team_key
WHERE nhl.date > now() - '3 years'::interval
ORDER BY
	game_id
"""
