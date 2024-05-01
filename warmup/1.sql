SELECT *
FROM movie_keyword mk,
    title t
WHERE t.id=mc.movie_id
AND t.production_year>2005;