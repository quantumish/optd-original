SELECT *
FROM title t,
    movie_info mi
WHERE t.id=mi.movie_id 
   AND t.production_year>2005;
