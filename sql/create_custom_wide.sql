--assumes you have run convert_plants.sql
CREATE VIEW ecosite_wide_custom AS
SELECT a.ecosite_id, a.ecosite_name, 
	   coalesce(b.symbol, a.plant_community, d.plants) dominant_plants,
	   coalesce(replace(c.tree_sci, ' - ', ', '), a.plant_comm_tree) dominant_tree,
	   coalesce(replace(c.shrub_sci, ' - ', ', '), a.plant_comm_shrub) dominant_shrub,
	   coalesce(replace(c.herb_sci, ' - ', ', '), a.plant_comm_herb) dominant_herb,
	   a.rsprod_total_l, a.rsprod_total_r, a.rsprod_total_h, 
	   coalesce(d.pz_l, a.map_l) pz_l, coalesce(d.pz_h, a.map_h) pz_h, a.map_r, 
	   a.ffd_r, a.geomdesc, a.elev_ft_l, a.elev_ft_h, slope_pct_l, slope_pct_h, 
	   nullif(a.aspect, 'NA') aspect, a.taxpartsize, a.pm,
	   nullif(concat_ws(' - ', ph1to1h2o_l, ph1to1h2o_h), '') ph,
	   nullif(concat_ws(' - ', a.drainagecl_l, a.drainagecl_h), '') drainagecl,
	   a.textures surf_texture
  FROM ecosite_wide a
  LEFT JOIN general_plants_condensed b ON a.ecosite_id = b.ecosite_id
  LEFT JOIN general_plants_gh c ON a.ecosite_id = c.ecosite_id
  LEFT JOIN general_info d ON a.ecosite_id = d.ecosite_id;
