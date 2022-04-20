query = """
CREATE TABLE IF NOT EXISTS team_stats(
	team_key varchar(15), 
	team char(3), 
	"ff%_5v5_last_half" double precision,
	"gf%_5v5_last_half" double precision,
	"xgf%_5v5_last_half" double precision,
	"sh%_5v5_last_half" double precision,
	gf_per_min_pp_last_half double precision,
	xgf_per_min_pp_last_half double precision,
	ga_per_min_pk_last_half double precision,
	xga_per_min_pk_last_half double precision,
	"date" date,
	b2b boolean,
	PRIMARY KEY (team_key) 
);

WITH support_calcs AS (
	SELECT *,
		--TOI 5v5
		SUM(toi_5v5) OVER (PARTITION BY team ORDER BY date ROWS BETWEEN 40 PRECEDING AND CURRENT ROW) as toi_5v5_last_half,
		--Fenwick 5v5
		SUM(ff_5v5) OVER (PARTITION BY team ORDER BY date ROWS BETWEEN 40 PRECEDING AND CURRENT ROW) as ff_5v5_last_half,
		SUM(fa_5v5) OVER (PARTITION BY team ORDER BY date ROWS BETWEEN 40 PRECEDING AND CURRENT ROW) as fa_5v5_last_half,
		--Goals 5v5
		SUM(gf_5v5) OVER (PARTITION BY team ORDER BY date ROWS BETWEEN 40 PRECEDING AND CURRENT ROW) as gf_5v5_last_half,
		SUM(ga_5v5) OVER (PARTITION BY team ORDER BY date ROWS BETWEEN 40 PRECEDING AND CURRENT ROW) as ga_5v5_last_half,
		--Expected Goals 5v5
		SUM(xgf_5v5) OVER (PARTITION BY team ORDER BY date ROWS BETWEEN 40 PRECEDING AND CURRENT ROW) as xgf_5v5_last_half,
		SUM(xga_5v5) OVER (PARTITION BY team ORDER BY date ROWS BETWEEN 40 PRECEDING AND CURRENT ROW) as xga_5v5_last_half,
		--Shots 5v5
		SUM(sf_5v5) OVER (PARTITION BY team ORDER BY date ROWS BETWEEN 40 PRECEDING AND CURRENT ROW) as sf_5v5_last_half,
		--PP
		SUM(toi_pp) OVER (PARTITION BY team ORDER BY date ROWS BETWEEN 40 PRECEDING AND CURRENT ROW) as toi_pp_last_half,
		SUM(xgf_pp) OVER (PARTITION BY team ORDER BY date ROWS BETWEEN 40 PRECEDING AND CURRENT ROW) as xgf_pp_last_half,
		SUM(gf_pp) OVER (PARTITION BY team ORDER BY date ROWS BETWEEN 40 PRECEDING AND CURRENT ROW) as gf_pp_last_half,
		--PK
		SUM(toi_pk) OVER (PARTITION BY team ORDER BY date ROWS BETWEEN 40 PRECEDING AND CURRENT ROW) as toi_pk_last_half,
		SUM(xga_pk) OVER (PARTITION BY team ORDER BY date ROWS BETWEEN 40 PRECEDING AND CURRENT ROW) as xga_pk_last_half,
		SUM(ga_pk) OVER (PARTITION BY team ORDER BY date ROWS BETWEEN 40 PRECEDING AND CURRENT ROW) as ga_pk_last_half
	
	FROM nst
)

INSERT INTO 
	team_stats(
			   team_key, 
			   team, 
			   "ff%_5v5_last_half",
			   "gf%_5v5_last_half",
			   "xgf%_5v5_last_half",
			   "sh%_5v5_last_half",
			   gf_per_min_pp_last_half,
			   xgf_per_min_pp_last_half,
			   ga_per_min_pk_last_half,
			   xga_per_min_pk_last_half,
			   date,
			   b2b
)

SELECT 
	team_key, 
	team, 
	(ff_5v5_last_half*100)/(ff_5v5_last_half+fa_5v5_last_half) as "ff%_5v5_last_half",
	(gf_5v5_last_half*100)/(gf_5v5_last_half+ga_5v5_last_half) as "gf%_5v5_last_half",
	(xgf_5v5_last_half*100)/(xgf_5v5_last_half+xga_5v5_last_half) as "xgf%_5v5_last_half",
	(gf_5v5_last_half*100)/sf_5v5_last_half as "sh%_5v5_last_half",
	gf_pp_last_half/toi_pp_last_half as gf_per_min_pp_last_half,
	xgf_pp_last_half/toi_pp_last_half as xgf_per_min_pp_last_half,
	ga_pk_last_half/toi_pk_last_half as ga_per_min_pk_last_half,
	xga_pk_last_half/toi_pk_last_half as xga_per_min_pk_last_half,
	date, 
	b2b

FROM support_calcs
--WHERE date > (SELECT "date" FROM team_stats ORDER BY "date" DESC LIMIT 1)
"""
