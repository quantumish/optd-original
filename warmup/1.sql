SELECT *
FROM movie_companies mc,
    movie_info_idx mi_idx
WHERE mi_idx.info_type_id=112
    AND mc.company_type_id=2;