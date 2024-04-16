DROP VIEW IF EXISTS plant_community;
CREATE VIEW plant_community AS
SELECT a.ecosite_id,
       coalesce(a.dominant_plants, b.plant_community, c.plants) dominant_plants,
       coalesce(a.dominant_tree, b.plant_comm_tree) dominant_tree,
	     coalesce(a.dominant_shrub, b.plant_comm_shrub) dominant_shrub,
	     coalesce(a.dominant_herb, b.plant_comm_herb) dominant_herb
  FROM general_plants a
  LEFT JOIN ecosite_wide b ON a.ecosite_id = b.ecosite_id
  LEFT JOIN general_info c ON a.ecosite_id = c.ecosite_id;
