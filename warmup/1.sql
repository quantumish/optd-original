SELECT *
FROM movie_companies mc,
    title t,
    movie_info_idx mi_idx
WHERE 
    AND mi_idx.info_type_id=112
    AND mc.company_type_id=2;