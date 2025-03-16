SELECT COUNT(*) AS ft_count, (SELECT COUNT(*) FROM vista_turismo_tutto) AS vs_count
FROM facts_turismo;

select * from facts_turismo ft;

CREATE OR REPLACE VIEW vista_turismo_tutto AS
select ft."REF_AREA", data_type.nome DATA_TYPE, tAccomodation.nome TYPE_ACCOMODATION, ateco2007.nome ECON_ACTIVITY_NACE_2007,
	countryResidence.nome COUNTRY_RES_GUESTS, localityType.nome LOCALITY_TYPE, uDegree.nome URBANIZ_DEGREE , cArea.nome COASTAL_AREA,
	sizeByRoom.nome SIZE_BY_NUMBER_ROOMS, ft."TIME_PERIOD", ft."OBS_VALUE"
from facts_turismo ft
left join dim_cl_tipo_dato7 data_type on ft."DATA_TYPE" = data_type.id
left join dim_cl_tipo_alloggio2 tAccomodation on ft."TYPE_ACCOMODATION"  = tAccomodation.id
left join dim_cl_ateco_2007 ateco2007 on ft."ECON_ACTIVITY_NACE_2007" = ateco2007.id
left join dim_cl_iso countryResidence on ft."COUNTRY_RES_GUESTS" = countryResidence.id
left join dim_cl_tipoitter1 localityType on ft."LOCALITY_TYPE" = localityType.id
left join dim_cl_tipoitter1 uDegree on ft."URBANIZ_DEGREE" = uDegree.id
left join dim_cl_tipoitter1 cArea on ft."COASTAL_AREA" = cArea.id
left join dim_cl_numerosita sizeByRoom on ft."SIZE_BY_NUMBER_ROOMS" = sizeByRoom.id;