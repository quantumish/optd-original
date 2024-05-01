SELECT *
FROM movie_companies mc,
    title t
WHERE t.id=mc.movie_id
    AND mc.company_type_id=2;