/*select count(*)
from facts_turismo ft
where ft."FREQ" != 'A';

select count(*)
from facts_indicatori_economici fie ;
select count(*)
from facts_indicatori_economici ft
left join dim_cl_itter107 refArea on ft."REF_AREA" = refArea.id
left join dim_cl_tipo_dato29 data_type on ft."DATA_TYPE" = data_type.id
left join dim_cl_ateco_2007 ateco2007 on ft."ECON_ACTIVITY_NACE_2007" = ateco2007.id;

select * from facts_indicatori_economici fie;*/

create view vista_indicatori_economici_tutto as
select refArea.nome REF_AREA, data_type.nome DATA_TYPE, ateco2007.nome ECON_ACTIVITY_NACE_2007, ft."TIME_PERIOD", ft."OBS_VALUE" 
from facts_indicatori_economici ft
left join dim_cl_itter107 refArea on ft."REF_AREA" = refArea.id
left join dim_cl_tipo_dato29 data_type on ft."DATA_TYPE" = data_type.id
left join dim_cl_ateco_2007 ateco2007 on ft."ECON_ACTIVITY_NACE_2007" = ateco2007.id;