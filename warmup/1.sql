SELECT *
FROM movie_keyword mk,
    title t
WHERE t.id=mk.movie_id
AND t.production_year>2005;