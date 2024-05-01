SELECT *
FROM title t,
    movie_keyword mk
WHERE t.id=mk.movie_id 
    AND mk.keyword_id=117;
