WITH base AS (
SELECT a.ecosite_id, a.ecosite_name, 
	   coalesce(b.dominant_plants, c.plants) dominant_plants, 
	   b.dominant_tree, b.dominant_shrub, b.dominant_herb,
	   a.rsprod_total_l, a.rsprod_total_r, a.rsprod_total_h, 
	   coalesce(c.pz_l, a.map_l) pz_l, coalesce(c.pz_h, a.map_h) pz_h, a.map_r, 
	   a.ffd_r, a.geomdesc, a.elev_ft_l, a.elev_ft_h, slope_pct_l, slope_pct_h, 
	   nullif(a.aspect, 'NA') aspect, a.taxpartsize, a.pm,
	   nullif(concat_ws(' - ', ph1to1h2o_l, ph1to1h2o_h), '') ph,
	   nullif(concat_ws(' - ', a.drainagecl_l, a.drainagecl_h), '') drainagecl,
	   a.textures surf_texture
  FROM ecosite_wide a
  LEFT JOIN plant_community b ON a.ecosite_id = b.ecosite_id
  LEFT JOIN general_info c ON a.ecosite_id = c.ecosite_id
)

SELECT * FROM base;
