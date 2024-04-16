#!/usr/bin/env python3
import os
import re
import json
import sys
import argparse
import pandas as pd
import numpy as np
import sqlite3 as sqlite


def scan_ecojson(path):
    p = re.compile(r'[A-Z]\d{3}[A-Z]{2}\d{3}[A-Z]{2}')
    ecolist = []
    for root, dirs, files in os.walk(path):
        for f in files:
            fsplit = os.path.splitext(f)
            fbase = fsplit[0]
            fext = fsplit[1]
            match = p.match(fbase)
            if match and fext == '.json':
                ecodict = {'ecosite_id': fbase}
                full_path = os.path.join(root, f)
                rel_path = full_path.replace(path, '').lstrip(os.path.sep)
                print("Reading", rel_path)
                with open(full_path, 'r' , encoding = 'utf-8') as jf:
                    d = json.load(jf)
                geninfo = d.get('generalInformation')
                if geninfo:
                    nar = geninfo.get('narratives')
                    if nar:
                        ecodict['ecosite_name_1'] = nar.get('ecoclassName')
                        ecodict['ecosite_name_2'] = nar.get('ecoclassSecondaryName')
                        ecodict['ecosite_name_3'] = nar.get('ecoclassTertiaryName')
                    asc_sites = geninfo.get('associatedSites')
                    asc_list = [x.get('symbol') for x in asc_sites]
                    ecodict['asc_sites'] = ';'.join(asc_list)
                    sim_sites = geninfo.get('similarSites')
                    sim_list = [x.get('symbol') for x in sim_sites]
                    ecodict['sim_sites'] = ';'.join(sim_list)
                    dom_species = geninfo.get('dominantSpecies')
                    if dom_species:
                        ecodict = ecodict | dom_species
                ecolist.append(ecodict)
    return ecolist


def convert_ecolist_df(ecolist, sp_df = None):
    # unfinished / unused
    #  if sp_df is not None:
    #      min_df = pd.DataFrame(eco_df,
    #                            columns=['ecosite_id', 'dominantTree1', 'dominantShrub1',
    #                                     'dominantHerb1', 'dominantTree2', 'dominantShrub2',
    #                                     'dominantHerb2'])
    #      dom_gh = pd.wide_to_long(min_df,
    #                               stubnames=['dominantTree', 'dominantShrub', 'dominantHerb'],
    #                               i = 'ecosite_id', j = 'rank').reset_index()

    #      dom_sp = pd.wide_to_long(dom_gh,
    #                               stubnames='dominant', suffix = r'[A-Za-z]{4,5}',
    #                               i = ['ecosite_id', 'rank'], j = 'gh').reset_index()
    edf = pd.DataFrame.from_dict(ecolist)
    names2 = edf['ecosite_name_2'].to_list()
    
    pz_search = [re.search(r'(\d+)\s*[\-\+]\s*(\d+)?"?\s*P\.?Z\.?', x) for x in names2]
    pz_groups = [x.groups() if x is not None else (None, None) for x in pz_search]
    pz_df = pd.DataFrame.from_records(pz_groups, columns =['pz_l', 'pz_h']).fillna(value=np.nan)
    pz_float = pz_df.apply(pd.to_numeric)
    ndf = edf.join(pz_float)

    plants_search =  [''.join(re.findall(r'([A-Za-z]{4,}\d*/?\-?)', x)) for x in names2]
    plant_repl = [x if x != '' else None for x in plants_search]
    plant_df = pd.DataFrame(plant_repl, columns =['plants'])
    nndf = ndf.join(plant_df).replace(r'^\s*$', None, regex=True)

    return nndf
         
           
if __name__ == "__main__":
    argv = sys.argv[1:]
    
    parser = argparse.ArgumentParser(description='Retrieve certain data in individual ecosite JSON'
                                                 ' files.')
    parser.add_argument('scanpath', help='directory to recursively scan for ecosite JSON files')
    parser.add_argument('outpath', 
                        help='path where the combined table will be saved with a ".csv" for CSV or '
                             'c(".db", ".sqlite") extention for SQLite.')
    #  species = parser.add_argument_group('species','USDA PLANTS species table import options')
    #  ex_species = species.add_mutually_exclusive_group()
    #  ex_species.add_argument('-s', '--species_csv',
    #                          help = 'path to a CSV file containing species information')

    #  ex_species.add_argument('-S', '--species_db',
    #                          help = 'path to a sqlite database containing species information')
    #  species.add_argument('-t', '--table_name',
    #                       help='in the case of a database path, name of the databse table to use')
    args = parser.parse_args(argv)

    elist = scan_ecojson(path=args.scanpath)

    #  if args.species_csv:
    #      species_df = pd.read_csv(args.species_csv)
    #  elif args.species_db:
    #      con = sqlite.connect(args.species_db)
    #      tbl = args.table_name
    #      sql = f'SELECT * FROM {tbl};'
    #      species_df = pd.read_sql(sql=sql, con=con)
    #  else:
    #      species_df = None

    eco_df = convert_ecolist_df(ecolist, sp_df = None): 

    if os.path.splitext(args.outpath)[1] == '.csv':
        eco_df.to_csv(args.outpath, index=False)
    else:
        con = sqlite.connect(args.outpath)
        nrows = eco_df.to_sql(name='general_info', con=con, index=False, if_exists='replace')
        con.close()

    print('\nScript finished.\n')

