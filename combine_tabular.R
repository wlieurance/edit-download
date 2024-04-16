#!/usr/bin/env Rscript
library(optparse)
library(dplyr)
library(tidyr)
library(readr)
library(stringr)
library(glue)
library(DBI)


read_edit_tsv <- function(file, mlra = FALSE, legacy = FALSE) {
  df <- read_tsv(file = file, skip = 2, show_col_types = FALSE) |>
    rename_with(~ str_to_lower(str_replace_all(.x, "\\s", "_"))) |>
    rename_with(~ str_replace(.x, "ecological_site", "ecosite"))
  if (mlra == FALSE) {
    df <- df |> select(!any_of("mlra"))
  }
  if (legacy == FALSE) {
    df <- df |> select(!any_of("ecosite_legacy_id"))
  }
  return(df)
}

combine_tables <- function(data_dir, db_path = NULL) {
  if (!is.null(db_path)) {
    con <- dbConnect(RSQLite::SQLite(), db_path)
  } else {
    con <- NULL
  }
  class_list <- read_edit_tsv(file.path(data_dir, "class-list.txt"),
                              mlra = TRUE, legacy = TRUE)
  if (!is.null(con)) {
    dbWriteTable(con, "class_list", class_list)
  }

  # annual production
  aprod <- read_edit_tsv(file.path(data_dir, "annual-production.txt"))
  if (!is.null(con)) {
    dbWriteTable(con, "annual_production", aprod)
  }
  aprod_gh <- aprod |>
    rename(l = production_low, h = production_high,
           r = production_rv) |>
    filter(land_use == 1 & ecosystem_state == 1 & plant_community == 1) |>
    select(!all_of(c("land_use", "ecosystem_state", "plant_community"))) |>
    mutate(plant_type =
             case_when(plant_type == "shrub/vine" ~ "shrub",
                       plant_type == "grass/grasslike" ~ "gram",
                       TRUE ~ plant_type))

  aprod_total <- aprod_gh |>
    group_by(ecosite_id) |>
    summarise(across(all_of(c("l", "r", "h")), ~ sum(.x, na.rm = TRUE)),
              .groups = "drop") |>
    mutate(plant_type = "total")

  aprod_all <- aprod_gh |> bind_rows(aprod_total)

  aprod_wide <- aprod_all |>
    pivot_wider(id_cols = c("ecosite_id"), names_from = "plant_type",
                values_from = c("l", "r", "h"),
                names_glue = "rsprod_{plant_type}_{.value}") |>
    select(ecosite_id, contains("total"), contains("shrub"),
           contains("tree"), contains("gram"), contains("forb"))

  # plant composition
  rcomp <- read_edit_tsv(file.path(data_dir, "rangeland-plant-composition.txt"))
  if (!is.null(con)) {
    dbWriteTable(con, "range_composition", rcomp)
  }

  rcomp_sp <- rcomp |>
    rename(l = production_low, h = production_high) |>
    filter(land_use == 1 & ecosystem_state == 1 & plant_community == 1) |>
    select(!all_of(c("land_use", "ecosystem_state", "plant_community"))) |>
    rowwise() |>
    mutate(r = mean(c_across(c("l", "h")), na.rm = TRUE)) |>
    ungroup() |>
    mutate(plant_type =
             case_when(plant_type == "shrub/vine" ~ "shrub",
                       plant_type == "grass/grasslike" ~ "gram",
                       TRUE ~ plant_type))

  rcomp_total <- rcomp_sp |>
    group_by(ecosite_id) |>
    summarize(r_sum = sum(r, na.rm = TRUE))

  rcomp_pct <- rcomp_sp |>
    inner_join(rcomp_total,
               by = c("ecosite_id" = "ecosite_id")) |>
    mutate(r_pct = r / r_sum,
           plant_grp =
             case_when(plant_type %in% c("gram", "forb") ~ "herb",
                       TRUE ~ plant_type)) |>

    filter(!is.na(r_pct)) |>
    group_by(ecosite_id, plant_grp) |>
    arrange(desc(r_pct), plant_symbol) |>
    mutate(grp_rnk = row_number()) |>
    ungroup() |>
    arrange(ecosite_id, plant_grp, desc(r_pct), plant_symbol)

  rcomp_filt <- rcomp_pct |>
    filter((plant_grp %in% c("herb", "shrub") & grp_rnk == 1) |
             (r_pct >= 0.10)) |>
    filter(grp_rnk <= 2)


  rcomp_wide <- rcomp_filt |>
    mutate(plant_grp_rnk = case_when(plant_grp == "tree" ~ 1,
                                     plant_grp == "shrub" ~ 2,
                                     plant_grp == "herb" ~ 3,
                                     TRUE ~ 4)) |>
    arrange(ecosite_id, plant_grp_rnk, grp_rnk) |>
    group_by(ecosite_id, plant_grp_rnk) |>
    summarize(code = paste0(plant_symbol, collapse = "-"), .groups = "drop") |>
    arrange(ecosite_id, plant_grp_rnk) |>
    group_by(ecosite_id) |>
    summarize(plant_community = paste0(code, collapse = "/"), .groups = "drop")

  rcomp_wide_cat <- rcomp_filt |>
    mutate(plant_grp_rnk = case_when(plant_grp == "tree" ~ 1,
                                     plant_grp == "shrub" ~ 2,
                                     plant_grp == "herb" ~ 3,
                                     TRUE ~ 4)) |>
    arrange(ecosite_id, plant_grp_rnk, grp_rnk) |>
    group_by(ecosite_id, plant_grp) |>
    summarize(sci_names = paste0(scientific_name, collapse = ", "),
              .groups = "drop") |>
    pivot_wider(id_cols = c("ecosite_id"), names_from = "plant_grp",
                values_from = "sci_names",
                names_glue = "plant_comm_{plant_grp}") |>
    select(ecosite_id, ends_with("tree"), ends_with("shrub"),
           ends_with("herb"))

  rcomp_final <- rcomp_wide |>
    inner_join(rcomp_wide_cat, by = c("ecosite_id"))


  # climatic-features
  clim_feat <- read_edit_tsv(file.path(data_dir, "climatic-features.txt"))
  if (!is.null(con)) {
    dbWriteTable(con, "climate_feat", clim_feat)
  }

  clim_feat_wide <- clim_feat |>
    rename(l = representative_low, h = representative_high,
           r = average) |>
    mutate(property =
             case_when(property == "mean annual precipitation" ~ "map",
                       property == "frost free days" ~ "ffd",
                       property == "freeze free days" ~ "frzfd",
                       TRUE ~ property)) |>
    pivot_wider(id_cols = c("ecosite_id"), names_from = "property",
                values_from = c("l", "r", "h"), 
                names_glue = "{property}_{.value}") |>
    select(ecosite_id, starts_with("map"), starts_with("ffd"),
           starts_with("frzfd"))

  # landforms
  landforms <- read_edit_tsv(file.path(data_dir, "landforms.txt"))
  if (!is.null(con)) {
    dbWriteTable(con, "landforms", landforms)
  }

  landforms_wide <- landforms |>
    select(!any_of(c("microfeature", "modifiers"))) |>
    group_by(ecosite_id, landscape) |>
    summarize(landforms = paste0(landform, collapse = ", "),
              .groups = "drop") |>
    mutate(landforms = case_when(!is.na(landscape) & !is.na(landforms) ~
                                   glue("({landforms})"),
                                 TRUE ~ landforms)) |>
    unite(landform, all_of(c("landscape", "landforms")), sep = " ",
          na.rm = TRUE) |>
    group_by(ecosite_id) |>
    summarize(geomdesc = paste0(landform, collapse = "; "),
              .groups = "drop")

  # physiographic interval
  phys_int <- read_edit_tsv(file.path(data_dir,
                                      "physiographic-interval-properties.txt"))
  if (!is.null(con)) {
    dbWriteTable(con, "phys_interval", phys_int)
  }
  phys_int_wide <- phys_int |>
    rename(l = representative_low, h = representative_high) |>
    mutate(property =
             case_when(property == "elevation" ~ "elev",
                       property == "ponding depth" ~ "ponddep",
                       property == "water table depth" ~ "wtdep",
                       TRUE ~ property),
           measurement_unit = ifelse(measurement_unit == "%", "pct",
                                     measurement_unit)) |>
    pivot_wider(id_cols = c("ecosite_id"),
                names_from = c("property", "measurement_unit"),
                values_from = c("l", "h"),
                names_glue = "{property}_{measurement_unit}_{.value}") |>
    select(ecosite_id, starts_with("elev"), starts_with("slope"),
           starts_with("pond"), starts_with("wt"))

  # physiographic nominal
  phys_nom <- read_edit_tsv(file.path(data_dir,
                                      "physiographic-nominal-properties.txt"))
  if (!is.null(con)) {
    dbWriteTable(con, "phys_nominal", phys_nom)
  }
  phys_nom_wide <- phys_nom |>
    mutate(property =
             case_when(property == "slope shape across" ~ "shapeacross",
                       property == "slope shape up-down" ~ "shapedown",
                       TRUE ~ property)) |>
    mutate(property_value = ifelse(property == "aspect",
                                   str_replace(property_value, "north", "N"),
                                   property_value)) |>
    mutate(property_value = ifelse(property == "aspect",
                                   str_replace(property_value, "south", "S"),
                                   property_value)) |>
    mutate(property_value = ifelse(property == "aspect",
                                   str_replace(property_value, "east", "E"),
                                   property_value)) |>
    mutate(property_value = ifelse(property == "aspect",
                                   str_replace(property_value, "west", "W"),
                                   property_value)) |>
    mutate(property_value = ifelse(property == "aspect",
                                   str_replace(property_value,
                                               "not applicable", NA_character_),
                                   property_value)) |>
    group_by(ecosite_id, property) |>
    summarize(values = paste0(property_value, collapse = ", "),
              .groups = "drop") |>
    pivot_wider(id_cols = c("ecosite_id"),
                names_from = "property",
                values_from = "values")


  # physiographic ordinal
  phys_ord <- read_edit_tsv(file.path(data_dir,
                                      "physiographic-ordinal-properties.txt"))
  if (!is.null(con)) {
    dbWriteTable(con, "phys_ordinal", phys_ord)
  }

  phys_ord_wide <- phys_ord |>
    rename(l = representative_low, h = representative_high) |>
    mutate(property =
             case_when(property == "flooding frequency" ~ "flodfreqcl",
                       property == "flooding duration" ~ "floddurcl",
                       property == "runoff class" ~ "runoff",
                       TRUE ~ property)) |>
    pivot_wider(id_cols = c("ecosite_id"),
                names_from = "property",
                values_from = c("l", "h"),
                names_glue = "{property}_{.value}") |>
    select(ecosite_id, starts_with("flodfreq"),
           starts_with("floddur"), starts_with("runoff"))

  # soil interval
  soil_int <- read_edit_tsv(file.path(data_dir,
                                      "soil-interval-properties.txt"))
  if (!is.null(con)) {
    dbWriteTable(con, "soil_interval", soil_int)
  }

  soil_int_wide <- soil_int |>
    rename(l = representative_low, h = representative_high) |>
    mutate(property =
             case_when(property == "depth to restrictive layer" ~
                         "resdep",
                       property == "soil depth" ~ "soildep",
                       property == "surface fragment cover <=3\"" ~
                         "sfragcov_less3",
                       property == "surface fragment cover >3\"" ~
                         "sfragcov_more3",
                       TRUE ~ property),
           measurement_unit = ifelse(measurement_unit == "%", "pct",
                                     measurement_unit)) |>
    pivot_wider(id_cols = c("ecosite_id"),
                names_from = c("property", "measurement_unit"),
                values_from = c("l", "h"),
                names_glue = "{property}_{measurement_unit}_{.value}") |>
    select(ecosite_id, starts_with("res"), starts_with("soil"),
           contains("less3"), contains("more3"))

  # soil nominal
  soil_nom <- read_edit_tsv(file.path(data_dir,
                                      "soil-nominal-properties.txt"))
  if (!is.null(con)) {
    dbWriteTable(con, "soil_nominal", soil_nom)
  }
  soil_nom_wide <- soil_nom |>
    mutate(property =
             case_when(property == "family particle size" ~ "taxpartsize",
                       TRUE ~ property)) |>
    group_by(ecosite_id, property) |>
    summarize(values = paste0(property_value, collapse = "; "),
              .groups = "drop") |>
    pivot_wider(id_cols = c("ecosite_id"),
                names_from = "property",
                values_from = c("values"))

  # soil ordinal
  soil_ord <- read_edit_tsv(file.path(data_dir,
                                      "soil-ordinal-properties.txt"))
  if (!is.null(con)) {
    dbWriteTable(con, "soil_ordinal", soil_ord)
  }

  soil_ord_wide <- soil_ord |>
    rename(l = representative_low, h = representative_high) |>
    mutate(property =
             case_when(property == "drainage class" ~ "drainagecl",
                       property == "permeability class" ~ "permcl",
                       TRUE ~ property)) |>
    pivot_wider(id_cols = c("ecosite_id"),
                names_from = "property",
                values_from = c("l", "h"),
                names_glue = "{property}_{.value}") |>
    select(ecosite_id, starts_with("drain"), starts_with("perm"))

  # parent material
  pm <- read_edit_tsv(file.path(data_dir, "soil-parent-material.txt"))
  if (!is.null(con)) {
    dbWriteTable(con, "pm", pm)
  }

  pm_wide <- pm |>
    mutate(origin = case_when(!is.na(kind) & !is.na(origin) ~
                                glue("({origin})"),
                              TRUE ~ origin)) |>
    unite(pms, all_of(c("kind", "origin")), sep = " ",
          na.rm = TRUE) |>
    group_by(ecosite_id) |>
    summarize(pm = paste0(pms, collapse = "; "),
              .groups = "drop")



  # soil profile
  soil_profile <- read_edit_tsv(file.path(data_dir,
                                          "soil-profile-properties.txt"))
  if (!is.null(con)) {
    dbWriteTable(con, "soil_profile", soil_profile)
  }

  soil_profile_wide <- soil_profile |>
    rename(l = representative_low, h = representative_high, dept = top_depth,
           depb = bottom_depth) |>
    mutate(property =
             case_when(property == "soil reaction (1:1 water)" ~
                         "ph1to1h2o",
                       property == "subsurface fragment volume <=3\"" ~
                         "subfragvol_less3",
                       property == "subsurface fragment volume >3\"" ~
                         "subfragvol_more3",
                       property == "available water capacity" ~
                         "awc",
                       property == "calcium carbonate equivalent" ~
                         "caco3",
                       property == "electrical conductivity" ~
                         "ec",
                       property == "sodium adsorption ratio" ~
                         "sar",
                       TRUE ~ property),
           measurement_unit = str_replace(measurement_unit, "%", "pct")) |>
    mutate(measurement_unit = str_replace(measurement_unit, "/", "_")) |>
    unite(prop_unit_cmb, all_of(c("property", "measurement_unit")), sep = "_",
          na.rm = TRUE) |>
    pivot_wider(id_cols = c("ecosite_id"),
                names_from = "prop_unit_cmb",
                values_from = c("l", "h", "dept", "depb"),
                names_glue = "{prop_unit_cmb}_{.value}") |>
    select(ecosite_id, starts_with("ph"), contains("less3"),
           contains("more3"), starts_with("awc"), starts_with("caco3"),
           starts_with("ec"), starts_with("sar"))


  # soil surface texture
  soil_stex <- read_edit_tsv(file.path(data_dir, "soil-surface-textures.txt"))
  if (!is.null(con)) {
    dbWriteTable(con, "soil_surf_text", soil_stex)
  }

  soil_stex_wide <- soil_stex |>
    select(!any_of(c("term_in_lieu"))) |>
    unite(texture, all_of(c("modifier_1", "modifier_2", "modifier_3",
                            "texture_class")), sep = " ", na.rm = TRUE) |>
    group_by(ecosite_id) |>
    summarize(textures = paste0(texture, collapse = "; "),
              .groups = "drop")

  full_df <- class_list |>
    left_join(aprod_wide,
              by = c("ecosite_id" = "ecosite_id")) |>
    left_join(rcomp_final,
              by = c("ecosite_id" = "ecosite_id")) |>
    left_join(clim_feat_wide,
              by = c("ecosite_id" = "ecosite_id")) |>
    left_join(landforms_wide,
              by = c("ecosite_id" = "ecosite_id")) |>
    left_join(phys_int_wide,
              by = c("ecosite_id" = "ecosite_id")) |>
    left_join(phys_nom_wide,
              by = c("ecosite_id" = "ecosite_id")) |>
    left_join(phys_ord_wide,
              by = c("ecosite_id" = "ecosite_id")) |>
    left_join(soil_int_wide,
              by = c("ecosite_id" = "ecosite_id")) |>
    left_join(soil_nom_wide,
              by = c("ecosite_id" = "ecosite_id")) |>
    left_join(soil_ord_wide,
              by = c("ecosite_id" = "ecosite_id")) |>
    left_join(pm_wide,
              by = c("ecosite_id" = "ecosite_id")) |>
    left_join(soil_profile_wide,
              by = c("ecosite_id" = "ecosite_id")) |>
    left_join(soil_stex_wide,
              by = c("ecosite_id" = "ecosite_id"))

  if (!is.null(con)) {
    dbWriteTable(con, "ecosite_wide", full_df)
    dbDisconnect(con)
  }
  return(full_df)
}


# run only if called from Rscript
if (sys.nframe() == 0) {

  description <- paste(
    "This script will combine certain tab delimited ecological tables",
    "downloaded from EDIT at the MLRA/geoUnit level contained in `data_dir`",
    "(e.g. class-list.txt, climatic-features.txt, etc.) into a single wide",
    "table saved to a database table or `out_file` in CSV format."
  )

  option_list <- list(
    make_option(c("-d", "--db_path"),
                help = "path to an SQLite database"),
    make_option(c("-s", "--save_long"), action="store_true", default=FALSE,
                help = paste("import long versions of individual tables into",
                             "database [default]")),
    make_option(c("-o", "--out_file"),
                help = "path to save a CSV version of the final wide table")
  )

  opt_parser <- optparse::OptionParser(
    usage = "usage: %prog data_dir",
    prog = NULL, description = description, option_list = option_list
  )

  args <- commandArgs(trailingOnly = TRUE)
  opt <- optparse::parse_args(opt_parser, positional_arguments = 1, args = args)

  wide_tbl <- combine_tables(data_dir = opt$args[1],
                             db_path = opt$options$db_path)
  if (!is.null(opt$options$out_file)) {
    write_csv(wide_tbl, file = opt$args[2], na = "")
  }
  print("Script finished.")
}
