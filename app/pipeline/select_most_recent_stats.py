query = """
SELECT ts.*
FROM team_stats ts
INNER JOIN 
	(SELECT team, MAX(date) AS last_played
	FROM team_stats
	GROUP BY team) most_recent
ON ts.team = most_recent.team 
AND ts.date = most_recent.last_played
"""
