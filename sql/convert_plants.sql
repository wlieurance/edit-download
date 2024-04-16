ATTACH DATABASE 'my_usda_plants.db' AS plants;
DROP TABLE IF EXISTS general_plants;
CREATE TABLE general_plants AS 
WITH domsp_long AS (
SELECT ecosite_id, 'tree' gh, 1 gh_rnk, 1 rnk, dominantTree1 sci_name FROM general_info UNION
SELECT ecosite_id, 'tree' gh, 1 gh_rnk, 2 rnk, dominantTree2 sci_name FROM general_info UNION
SELECT ecosite_id, 'shrub' gh, 2 gh_rnk, 1 rnk, dominantShrub1 sci_name FROM general_info UNION
SELECT ecosite_id, 'shrub' gh, 2 gh_rnk, 2 rnk, dominantShrub2 sci_name FROM general_info UNION
SELECT ecosite_id, 'herb' gh, 3 gh_rnk, 1 rnk, dominantHerb1 sci_name FROM general_info UNION
SELECT ecosite_id, 'herb' gh, 3 gh_rnk, 2 rnk, dominantHerb2 sci_name FROM general_info

), domsp_filt AS (
SELECT * FROM domsp_long WHERE sci_name != '' AND sci_name IS NOT NULL

), accepted AS (
SELECT a.*, b.accepted_symbol
  FROM domsp_filt a
  INNER JOIN plants.plants b ON a.sci_name = b.scientific_name

), code_cmb AS ( 
SELECT ecosite_id, gh, gh_rnk, string_agg(accepted_symbol, '-' ORDER BY rnk) code
  FROM accepted
  GROUP BY ecosite_id, gh, gh_rnk

), gh_cmb AS (
--final table for plant codes
SELECT ecosite_id, string_agg(code, '/' ORDER BY gh_rnk) dominant_plants
  FROM code_cmb
 GROUP BY ecosite_id

), gh_wide AS (
SELECT ecosite_id, 
       nullif(concat_ws(', ', dominantTree1, dominantTree2), '') dominant_tree,
	   nullif(concat_ws(', ', dominantShrub1, dominantShrub2), '') dominant_shrub,
	   nullif(concat_ws(', ', dominantHerb1, dominantHerb2), '') dominant_herb
  FROM general_info

), final_join AS (

SELECT a.ecosite_id, b.dominant_plants, a.dominant_tree, a.dominant_shrub,
       a.dominant_herb
  FROM gh_wide a
  LEFT JOIN gh_cmb b ON a.ecosite_id = b.ecosite_id
)

SELECT * FROM final_join;
DETACH DATABASE plants;
