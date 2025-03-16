SELECT COUNT(*) AS ft_count, (SELECT COUNT(*) FROM vista_pernottamenti_tutto) AS vs_count
FROM facts_pernottamenti fp ;

select * from facts_pernottamenti fp;

create or replace view vista_pernottamenti_tutto as
select resTerr.nome RESIDENCE_TERR, data_type.nome DATA_TYPE, ft."MAIN_DESTINATION", tTrip.nome TYPE_TRIP,
tAccomodation.nome MAIN_TYPE_ACCOMODATION, sex.nome SEX, age.nome AGE, lStatus.nome LABPROF_STATUS_C, ft."TIME_PERIOD", ft."OBS_VALUE"
from facts_pernottamenti ft
left join dim_cl_itter107 resTerr on ft."RESIDENCE_TERR" = resTerr.id
left join dim_cl_tipo_dato_viaggi data_type on ft."DATA_TYPE" = data_type.id
left join dim_cl_iso mDest on ft."MAIN_DESTINATION" = mDest.id
left join dim_cl_tipo_viaggio2 tTrip on ft."TYPE_TRIP" = tTrip.id
left join dim_cl_tipo_alloggio tAccomodation on ft."MAIN_TYPE_ACCOMODATION" = tAccomodation.id
left join dim_cl_sexistat1 sex on ft."SEX" = sex.id
left join dim_cl_eta1 age on ft."AGE" = age.id
left join dim_cl_condizione_dich2 lStatus on ft."LABPROF_STATUS_C" = lStatus.id;