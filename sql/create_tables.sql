CREATE TABLE annual_production (
  mlra TEXT,
  ecosite_id TEXT,
  ecosite_legacy_id TEXT,
  land_use REAL,
  ecosystem_state REAL,
  plant_community REAL,
  plant_type TEXT,
  production_low REAL,
  production_rv REAL,
  production_high REAL,
  PRIMARY KEY (ecosite_id, land_use, ecosystem_state, plant_community, plant_type)
);

CREATE TABLE class_list (
  mlra TEXT,
  ecosite_id TEXT,
  ecosite_legacy_id TEXT,
  ecosite_name TEXT,
  PRIMARY KEY(ecosite_id)
);

CREATE TABLE climatic_features (
  mlra TEXT,
  ecosite_id TEXT,
  ecosite_legacy_id TEXT,
  property TEXT,
  measurement_unit INTEGER,
  representative_low REAL,
  representative_high REAL,
  range_low REAL,
  range_high REAL,
  average REAL,
  PRIMARY KEY (ecosite_id, property)
);

CREATE TABLE forest_overstory (
  mlra TEXT,
  ecosite_id TEXT,
  ecosite_legacy_id TEXT,
  land_use TEXT,
  ecosystem_state TEXT,
  plant_community TEXT,
  plant_type TEXT,
  plant_symbol TEXT,
  common_name TEXT,
  scientific_name TEXT,
  canopy_cover_low TEXT,
  canopy_cover_high TEXT,
  canopy_bottom_height TEXT,
  canopy_top_height TEXT,
  tree_diameter_low TEXT,
  tree_diameter_high TEXT,
  tree_basal_area_low TEXT,
  tree_basal_area_high TEXT
);

-- due to null nature of canopy_top_height field, we do unique index instead of primary key
-- tree_diameter was tried first, but failed.  canopy_top_height so far is more consistent for UNIQUE
CREATE UNIQUE INDEX fo_canopy_top_height_uix 
    ON forest_overstory(
  ecosite_id, land_use, ecosystem_state, plant_community, plant_type, plant_symbol, canopy_top_height);

CREATE TABLE forest_understory (
  mlra TEXT,
  ecosite_id TEXT,
  ecosite_legacy_id TEXT,
  land_use TEXT,
  ecosystem_state TEXT,
  plant_community TEXT,
  plant_type TEXT,
  plant_symbol TEXT,
  common_name TEXT,
  scientific_name TEXT,
  canopy_cover_low TEXT,
  canopy_cover_high TEXT,
  canopy_bottom_height TEXT,
  canopy_top_height TEXT 
);

-- due to null nature of canopy_top_height field, we do unique index instead of primary key
-- tree_diameter was tried first, but failed, canopy_top_height so far is more consistent for UNIQUE
CREATE UNIQUE INDEX fu_canopy_top_height_uix 
    ON forest_understory(
  ecosite_id, land_use, ecosystem_state, plant_community, plant_type, plant_symbol, canopy_top_height);

CREATE TABLE landforms (
  mlra TEXT,
  ecosite_id TEXT,
  ecosite_legacy_id TEXT,
  sequence REAL,
  landscape TEXT,
  landform TEXT,
  microfeature INTEGER,
  modifiers INTEGER,
  PRIMARY KEY (ecosite_id, sequence)
);


CREATE TABLE model_state_narratives (
  mlra TEXT,
  ecosite_id TEXT,
  ecosite_legacy_id TEXT,
  state_type TEXT,
  land_use REAL,
  ecosystem_state REAL,
  plant_community REAL,
  name TEXT,
  description TEXT,
  characteristics_and_indicators INTEGER,
  management TEXT
);

-- due to null nature of plant_community field, we do unique index instead of primary key
CREATE UNIQUE INDEX msn_plant_community_uix 
    ON model_state_narratives(
  ecosite_id, land_use, ecosystem_state, plant_community);


CREATE TABLE model_transition_narratives (
  mlra TEXT,
  ecosite_id TEXT,
  ecosite_legacy_id TEXT,
  transition_type TEXT,
  from_land_use REAL,
  from_ecosystem_state REAL,
  from_plant_community REAL,
  to_land_use REAL,
  to_ecosystem_state REAL,
  to_plant_community REAL,
  name TEXT,
  mechanism TEXT,
  recovery_constraints TEXT,
  context_dependence TEXT,
  legend TEXT
);

-- due to null nature of plant_community field, we do unique index instead of primary key
-- the mechanism field should in theory not be needed, but some ecological sites seem
-- to be carrying duplicates where mechanism is NULL for one and not the other.
CREATE UNIQUE INDEX mtn_mechanism_uix 
    ON model_transition_narratives(
  ecosite_id, transition_type,
  from_land_use, from_ecosystem_state, from_plant_community, 
  to_land_use, to_ecosystem_state, to_plant_community, name, mechanism);

CREATE TABLE physiographic_interval_properties (
  mlra TEXT,
  ecosite_id TEXT,
  ecosite_legacy_id TEXT,
  property TEXT,
  measurement_unit TEXT,
  representative_low REAL,
  representative_high REAL,
  range_low REAL,
  range_high REAL,
  PRIMARY KEY (ecosite_id, property)
);

CREATE TABLE physiographic_nominal_properties (
  mlra TEXT,
  ecosite_id TEXT,
  ecosite_legacy_id TEXT,
  property TEXT,
  property_value TEXT,
  PRIMARY KEY (ecosite_id, property, property_value)
);

CREATE TABLE physiographic_ordinal_properties (
  mlra TEXT,
  ecosite_id TEXT,
  ecosite_legacy_id TEXT,
  property TEXT,
  representative_low TEXT,
  representative_high TEXT,
  range_low INTEGER,
  range_high INTEGER,
  PRIMARY KEY (ecosite_id, property)
);

-- we shouldnt need to add production low/high into the primary key, but due to probable
-- data errors in th source, we will
CREATE TABLE rangeland_plant_composition (
  mlra TEXT,
  ecosite_id TEXT,
  ecosite_legacy_id TEXT,
  land_use REAL,
  ecosystem_state REAL,
  plant_community REAL,
  group_number REAL,
  group_label TEXT,
  custom_group_number REAL,
  plant_type TEXT,
  plant_symbol TEXT,
  common_name TEXT,
  scientific_name TEXT,
  production_low REAL,
  production_high REAL,
  foliar_cover_low REAL,
  foliar_cover_high REAL,
  PRIMARY KEY (ecosite_id, land_use, ecosystem_state, plant_community, 
               group_number, plant_symbol, production_low, production_high)
);

CREATE TABLE soil_interval_properties (
  mlra TEXT,
  ecosite_id TEXT,
  ecosite_legacy_id TEXT,
  property TEXT,
  measurement_unit TEXT,
  representative_low REAL,
  representative_high REAL,
  range_low REAL,
  range_high REAL,
  PRIMARY KEY (ecosite_id, property)
);

CREATE TABLE soil_nominal_properties (
  mlra TEXT,
  ecosite_id TEXT,
  ecosite_legacy_id TEXT,
  property TEXT,
  property_value TEXT,
  PRIMARY KEY (ecosite_id, property, property_value)
);

CREATE TABLE soil_ordinal_properties (
  mlra TEXT,
  ecosite_id TEXT,
  ecosite_legacy_id TEXT,
  property TEXT,
  representative_low TEXT,
  representative_high TEXT,
  range_low INTEGER,
  range_high INTEGER,
  PRIMARY KEY (ecosite_id, property)
);

CREATE TABLE soil_parent_material (
  mlra TEXT,
  ecosite_id TEXT,
  ecosite_legacy_id TEXT,
  sequence REAL,
  kind TEXT,
  origin TEXT,
  PRIMARY KEY (ecosite_id, sequence)
);

CREATE TABLE soil_profile_properties (
  mlra TEXT,
  ecosite_id TEXT,
  ecosite_legacy_id TEXT,
  property TEXT,
  measurement_unit TEXT,
  top_depth REAL,
  bottom_depth REAL,
  representative_low REAL,
  representative_high REAL,
  range_low INTEGER,
  range_high INTEGER
);

-- due to null nature of top_depth field, we do unique index instead of primary key
-- in theory top depth only would be sufficient, but due to probable errors in the dataset
-- bottom depth too is required for UNIQUE
CREATE UNIQUE INDEX spp_bottom_depth_uix 
    ON soil_profile_properties(
  ecosite_id, property, top_depth, bottom_depth);

CREATE TABLE soil_surface_cover (
  mlra TEXT,
  ecosite_id TEXT,
  ecosite_legacy_id TEXT,
  land_use TEXT,
  ecosystem_state TEXT,
  plant_community TEXT,
  cover_type TEXT,
  cover_low TEXT,
  cover_high TEXT,
  PRIMARY KEY (ecosite_id, land_use, ecosystem_state, plant_community, cover_type)
);

CREATE TABLE soil_surface_textures (
  mlra TEXT,
  ecosite_id TEXT,
  ecosite_legacy_id TEXT,
  sequence REAL,
  texture_class TEXT,
  modifier_1 TEXT,
  modifier_2 INTEGER,
  modifier_3 INTEGER,
  term_in_lieu REAL,
  PRIMARY KEY (ecosite_id, sequence)
);

--
-- custom tables
--

CREATE TABLE ecosite_wide (
  mlra TEXT,
  ecosite_id TEXT,
  ecosite_legacy_id TEXT,
  ecosite_name TEXT,
  rsprod_total_l REAL,
  rsprod_total_r REAL,
  rsprod_total_h REAL,
  rsprod_shrub_l REAL,
  rsprod_shrub_r REAL,
  rsprod_shrub_h REAL,
  rsprod_tree_l REAL,
  rsprod_tree_r REAL,
  rsprod_tree_h REAL,
  rsprod_gram_l REAL,
  rsprod_gram_r REAL,
  rsprod_gram_h REAL,
  rsprod_forb_l REAL,
  rsprod_forb_r REAL,
  rsprod_forb_h REAL,
  plant_community TEXT,
  plant_comm_tree TEXT,
  plant_comm_shrub TEXT,
  plant_comm_herb TEXT,
  map_l REAL,
  map_r REAL,
  map_h REAL,
  ffd_l REAL,
  ffd_r REAL,
  ffd_h REAL,
  frzfd_l REAL,
  frzfd_r REAL,
  frzfd_h REAL,
  geomdesc TEXT,
  elev_ft_l REAL,
  elev_ft_h REAL,
  slope_pct_l REAL,
  slope_pct_h REAL,
  ponddep_in_l REAL,
  ponddep_in_h REAL,
  wtdep_in_l REAL,
  wtdep_in_h REAL,
  aspect TEXT,
  shapeacross TEXT,
  shapedown TEXT,
  flodfreqcl_l TEXT,
  flodfreqcl_h TEXT,
  floddurcl_l TEXT,
  floddurcl_h TEXT,
  runoff_l TEXT,
  runoff_h TEXT,
  resdep_in_l REAL,
  resdep_in_h REAL,
  soildep_in_l REAL,
  soildep_in_h REAL,
  sfragcov_less3_pct_l REAL,
  sfragcov_less3_pct_h REAL,
  sfragcov_more3_pct_l REAL,
  sfragcov_more3_pct_h REAL,
  taxpartsize TEXT,
  drainagecl_l TEXT,
  drainagecl_h TEXT,
  permcl_l TEXT,
  permcl_h TEXT,
  pm TEXT,
  ph1to1h2o_l REAL,
  ph1to1h2o_h REAL,
  ph1to1h2o_dept REAL,
  ph1to1h2o_depb REAL,
  subfragvol_less3_pct_l REAL,
  subfragvol_less3_pct_h REAL,
  subfragvol_less3_pct_dept REAL,
  subfragvol_less3_pct_depb REAL,
  subfragvol_more3_pct_l REAL,
  subfragvol_more3_pct_h REAL,
  subfragvol_more3_pct_dept REAL,
  subfragvol_more3_pct_depb REAL,
  awc_in_l REAL,
  awc_in_h REAL,
  awc_in_dept REAL,
  awc_in_depb REAL,
  caco3_pct_l REAL,
  caco3_pct_h REAL,
  caco3_pct_dept REAL,
  caco3_pct_depb REAL,
  ec_mmhos_cm_l REAL,
  ec_mmhos_cm_h REAL,
  ec_mmhos_cm_dept REAL,
  ec_mmhos_cm_depb REAL,
  sar_l REAL,
  sar_h REAL,
  sar_dept REAL,
  sar_depb REAL,
  textures TEXT,
  hillslope_profile TEXT,
  geomorphic_position_terraces TEXT,
  geomorphic_position_hills TEXT,
  geomorphic_position_mountains TEXT,
  PRIMARY KEY (ecosite_id)
);


CREATE TABLE general_info (
  ecosite_id TEXT,
  mlra TEXT,
  ecosite_name_1 TEXT,
  ecosite_name_2 TEXT,
  ecosite_name_3 TEXT,
  asc_sites TEXT,
  sim_sites TEXT,
  dominantTree1 TEXT,
  dominantShrub1 TEXT,
  dominantHerb1 TEXT,
  dominantTree2 TEXT,
  dominantShrub2 TEXT,
  dominantHerb2 TEXT,
  pz_l REAL,
  pz_h REAL,
  plants TEXT,
  PRIMARY KEY (ecosite_id)
);

CREATE TABLE sites_associated (
mlra TEXT,
  ecosite_id TEXT,
  asc_site TEXT,
  PRIMARY KEY (ecosite_id, asc_site)
);

CREATE TABLE sites_similar (
mlra TEXT,
  ecosite_id TEXT,
  sim_site TEXT,
  PRIMARY KEY (ecosite_id, sim_site)
);
