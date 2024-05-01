SELECT *
FROM movie_companies mc,
    title t
WHERE t.id=mc.movie_id;