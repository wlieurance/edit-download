--ATTACH '/home/wlieurance/network/lab/_Public/ExternalData/Flora/USDA/PLANTS/Database/plants.sqlite' AS plants;
DROP TABLE IF EXISTS general_plants;
CREATE TABLE general_plants AS
SELECT ecosite_id, ecosite_name_1 ecosite_name,
       trim(concat_ws(' / ',
	     concat_ws(' - ', a.dominantTree1, a.dominantTree2),
	     concat_ws(' - ', a.dominantShrub1, a.dominantShrub2)) || ' / ' ||
	     concat_ws(' - ', a.dominantHerb1, a.dominantHerb2)) dominant_plants,
	   trim(concat_ws(' / ',
	     concat_ws(' - ', b.common_name, c.common_name),
	     concat_ws(' - ', d.common_name, e.common_name)) || ' / ' ||
	     concat_ws(' - ', f.common_name, g.common_name)) dominant_common,
	   concat_ws('/',
	     concat_ws('-', b.accepted_symbol, c.accepted_symbol),
	     concat_ws('-', d.accepted_symbol, e.accepted_symbol)) || '/' ||
	     concat_ws('-', f.accepted_symbol, g.accepted_symbol) dominant_codes
  FROM general_info a
  LEFT JOIN plants.plants b ON a.DominantTree1 = b.scientific_name
  LEFT JOIN plants.plants c ON a.DominantTree2 = c.scientific_name
  LEFT JOIN plants.plants d ON a.DominantShrub1 = d.scientific_name
  LEFT JOIN plants.plants e ON a.DominantShrub2 = e.scientific_name
  LEFT JOIN plants.plants f ON a.DominantHerb1 = f.scientific_name
  LEFT JOIN plants.plants g ON a.DominantHerb2 = g.scientific_name

--DETACH plants;
