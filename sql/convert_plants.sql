-- Assumes you have run create_plants.sql

DROP VIEW IF EXISTS general_plants;
CREATE VIEW general_plants AS
WITH domsp_long AS (
SELECT ecosite_id, 'tree' growth_habit, 1 gh_rank, 1 plant_rank, dominantTree1 sci_name FROM general_info UNION
SELECT ecosite_id, 'tree' growth_habit, 1 gh_rank, 2 plant_rank, dominantTree2 sci_name FROM general_info UNION
SELECT ecosite_id, 'shrub' growth_habit, 2 gh_rank, 1 plant_rank, dominantShrub1 sci_name FROM general_info UNION
SELECT ecosite_id, 'shrub' growth_habit, 2 gh_rank, 2 plant_rank, dominantShrub2 sci_name FROM general_info UNION
SELECT ecosite_id, 'herb' growth_habit, 3 gh_rank, 1 plant_rank, dominantHerb1 sci_name FROM general_info UNION
SELECT ecosite_id, 'herb' growth_habit, 3 gh_rank, 2 plant_rank, dominantHerb2 sci_name FROM general_info

), domsp_filt AS (
SELECT * FROM domsp_long WHERE sci_name != '' AND sci_name IS NOT NULL

), accepted AS (
SELECT a.*, b.accepted_symbol
  FROM domsp_filt a
  LEFT JOIN plants b ON replace(a.sci_name, 'subsp.', 'ssp.') = b.scientific_name

), accepted_convert AS (
SELECT a.ecosite_id, a.growth_habit, a.gh_rank, a.plant_rank, a.sci_name orig_name, 
       a.accepted_symbol, b.scientific_name, b.common_name
  FROM accepted a
  LEFT JOIN plants b ON a.accepted_symbol = b.symbol
  ORDER BY ecosite_id, gh_rank, plant_rank
)

SELECT * FROM accepted_convert;

----

DROP VIEW IF EXISTS general_plants_semicondensed;
CREATE VIEW general_plants_semicondensed AS 
SELECT ecosite_id, growth_habit, gh_rank,
       string_agg(orig_name, ' - ' ORDER BY plant_rank) scientific_name,
	   string_agg(common_name, ' - ' ORDER BY plant_rank) common_name,
       string_agg(accepted_symbol, '-' ORDER BY plant_rank) symbol
  FROM general_plants
  GROUP BY ecosite_id, growth_habit, gh_rank;

----

DROP VIEW IF EXISTS general_plants_condensed;
CREATE VIEW general_plants_condensed AS
SELECT ecosite_id, 
       string_agg(scientific_name, ' / ' ORDER BY gh_rank) scientific_name,
	     string_agg(common_name, ' / ' ORDER BY gh_rank) common_name,
	     string_agg(symbol, '/' ORDER BY gh_rank) symbol
  FROM general_plants_semicondensed
 GROUP BY ecosite_id;

----

DROP VIEW IF EXISTS general_plants_gh;
CREATE VIEW general_plants_gh AS
WITH tree AS (
SELECT * FROM general_plants_semicondensed WHERE growth_habit = 'tree'

), shrub AS (
SELECT * FROM general_plants_semicondensed WHERE growth_habit = 'shrub'

), herb AS (
SELECT * FROM general_plants_semicondensed WHERE growth_habit = 'herb'

), base AS (
SELECT a.ecosite_id, 
       b.scientific_name tree_sci, b.common_name tree_com, b.symbol tree_sym,
	     c.scientific_name shrub_sci, c.common_name shrub_com, c.symbol shrub_sym,
	     d.scientific_name herb_sci, d.common_name herb_com, d.symbol herb_sym
  FROM class_list a
  LEFT JOIN tree b ON a.ecosite_id = b.ecosite_id
  LEFT JOIN shrub c ON a.ecosite_id = c.ecosite_id
  LEFT JOIN herb d ON a.ecosite_id = d.ecosite_id
)

SELECT * FROM base;
