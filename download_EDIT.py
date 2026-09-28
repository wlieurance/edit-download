#!/usr/bin/env python3
import requests
import argparse
import json
import os
import sys
import re
from pathlib import Path

TIMEOUT = 60
OLD_URL = r'https://edit.jornada.nmsu.edu'
BASE_URL = r'https://edit.sc.egov.usda.gov'


def get_ecolist(path):
    with open(path, 'r', encoding='utf8') as f:
        lines = [line.rstrip().strip("\"\'") for line in f]
    return lines

def get_from_edit(ecolist, save_path):
    l = f'{BASE_URL}/services/descriptions/{catalog}/{geoUnit}/{ecoclass}'
    lp = f'{BASE_URL}/services/downloads/{catalog}/{geoUnit}/{item}'
    esd_pat = re.compile(r'^[FRG](\d{3}[A-Z]).+')
    for ecoclass in ecolist:
        print('Downloading ', ecoclass, '...', sep = '')
        var_dict = {'catalog': 'esd', 'ecoclass': ecoclass}
        matches = esd_pat.findall(ecoclass)
        if matches:
            var_dict['geoUnit'] = matches[0]
            link = '.'.join((l.format(**var_dict), 'json'))
            r = requests.get(link, headers={'Accept': 'application/json'}, timeout=TIMEOUT)
            eco_json = r.json()
            if eco_json.keys():
                if list(eco_json.keys())[0] != 'error':
                    # create folder
                    new_dir = os.path.join(save_path, ecoclass)
                    Path(new_dir).mkdir(parents=True, exist_ok=True)

                    # write json
                    json_fname = '.'.join((ecoclass, 'json'))
                    json_path = os.path.join(new_dir, json_fname)
                    with open(json_path, 'w', encoding='utf-8') as f:
                        json.dump(eco_json, f, ensure_ascii=False, indent = 4)
                    print("\tJSON success.")

                    # write pdf
                    link_pdf = '.'.join((l.format(**var_dict), 'pdf'))
                    pdf_fname = '.'.join((ecoclass, 'pdf'))
                    pdf_path = os.path.join(new_dir, pdf_fname)
                    r_pdf = requests.get(link_pdf, timeout=TIMEOUT)
                    with open(pdf_path, 'wb') as f:
                        f.write(r_pdf.content)
                    print("\tPDF success.")

                    # write production
                    var_dict['item'] = 'annual-production.txt'
                    link_prod = lp.format(**var_dict)
                    prod_fname = '_'.join((var_dict['geoUnit'], var_dict['item']))
                    prod_path = os.path.join(save_path, prod_fname)
                    r_prod = requests.get(link_prod, timeout=TIMEOUT)
                    with open(prod_path, 'w', encoding='utf-8') as f:
                        f.write(r_prod.text)
                    print("\tProduction success.")

                else:
                    print('\t', eco_json, sep = '')
            else:
                print('\tEmpty response.')
        else:
            print('\tCouldnt match geoUnit.')


def send_request(link, path, save = True):
    l_ext = os.path.splitext(link)[1]
    if l_ext == '.txt':
        headers = {'Accept': 'text/tab-separated-values'}
    elif l_ext == '.json':
        headers = {'Accept': 'application/json'}
    elif l_ext == '.pdf':
        headers={'Accept': 'application/pdf'}
    else:
        print(l_ext, 'not one of [.txt, .pdf, .json]')
        return None

    r = requests.get(link, headers = headers, timeout=TIMEOUT)
    if not r:
        print('Could not retrieve content.')
        return r
    if r.status_code == 404:
        print('Page not found.')
        return r

    if save:
        print('Saving', path)
        if l_ext == '.txt':
            with open(path, 'w', encoding='utf-8') as f:
                f.write(r.text)
        elif l_ext == '.json':
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(r.json(), f, ensure_ascii=False, indent = 4)
        elif l_ext == '.pdf':
            with open(path, 'wb') as f:
                f.write(r.content)
    return r


def get_catalog(path, catalog = 'esd', save = False):
    geo_unit_list = None
    base_link = f'{BASE_URL}/services/downloads/{catalog}/'
    links = ['geo-unit-list.json']
    add_links = ['geo-unit-list.txt',
                 'class-list.txt']
    if save:
        links.extend(add_links)
    out_dir = os.path.join(path, catalog)
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    for l in links:
        link = ''.join((base_link, l)).format(catalog = catalog)
        fname = os.path.basename(link)
        out_path = os.path.join(out_dir, fname)
        r = send_request(link = link, path = out_path, save = save)
        if l == 'geo-unit-list.json':
            if r:
                if r.status_code != 404:
                    geo_unit_list = r.json()
    return geo_unit_list



def get_geoUnit(geoUnit, path, catalog = 'esd', save = True):
    class_list = None
    base_text = f'{BASE_URL}/services/downloads/{catalog}/'
    base_pdf = f'{BASE_URL}/services/descriptions/{catalog}/'
    links = ['{geoUnit}/class-list.json']
    add_links = ['{geoUnit}/class-list.txt',
                 '{geoUnit}/climatic-features.txt',
                 '{geoUnit}/landforms.txt',
                 '{geoUnit}/physiographic-interval-properties.txt',
                 '{geoUnit}/physiographic-nominal-properties.txt',
                 '{geoUnit}/physiographic-ordinal-properties.txt',
                 '{geoUnit}/annual-production.txt',
                 '{geoUnit}/forest-overstory.txt',
                 '{geoUnit}/forest-understory.txt',
                 '{geoUnit}/rangeland-plant-composition.txt',
                 '{geoUnit}/soil-surface-cover.txt',
                 '{geoUnit}/soil-parent-material.txt',
                 '{geoUnit}/soil-interval-properties.txt',
                 '{geoUnit}/soil-nominal-properties.txt',
                 '{geoUnit}/soil-ordinal-properties.txt',
                 '{geoUnit}/soil-profile-properties.txt',
                 '{geoUnit}/soil-surface-textures.txt',
                 '{geoUnit}/model-state-narratives.txt',
                 '{geoUnit}/model-transition-narratives.txt',
                 '{geoUnit}.pdf']
    if save:
        links.extend(add_links)
    out_dir = os.path.join(path, catalog, geoUnit)
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    for l in links:
        if os.path.splitext(l)[1] == '.pdf':
            base = base_pdf
        else:
            base = base_text
        l_full = ''.join((base, l))
        link = l_full.format(catalog = catalog, geoUnit = geoUnit)
        fname = os.path.basename(link)
        out_path = os.path.join(out_dir, fname)
        r = send_request(link = link, path = out_path, save = save)
        if l == '{geoUnit}/class-list.json':
            if r:
                if r.status_code != 404:
                    class_list = r.json()
    return class_list


def get_ecoclass(ecoclass, geoUnit, path, catalog = 'esd', save = True, aux = True):
    state_list = None
    base_desc = f'{BASE_URL}/services/descriptions/{catalog}/{geoUnit}/'
    base_model = f'{BASE_URL}/services/models/{catalog}/{geoUnit}/'
    links = ['{ecoclass}/states.json']
    add_links = ['{ecoclass}.json',
             '{ecoclass}/overview.json',
             '{ecoclass}/climatic-features.json',
             '{ecoclass}/ecological-dynamics.json',
             '{ecoclass}/general-information.json',
             '{ecoclass}/interpretations.json',
             '{ecoclass}/physiographic-features.json',
             '{ecoclass}/reference-sheet.json',
             '{ecoclass}/soil-features.json',
             '{ecoclass}/transitions.json',
             '{ecoclass}/supporting-information.json',
             '{ecoclass}/water-features.json',
             '{ecoclass}.pdf',
             ]
    if save:
        links.extend(add_links)
    out_dir = os.path.join(path, catalog, geoUnit, ecoclass)
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    for l in links:
        if l in ['{ecoclass}/states.json', '{ecoclass}/transitions.json']:
            base = base_model
        else:
            base = base_desc
        l_full = ''.join((base, l))
        link = l_full.format(catalog = catalog, geoUnit = geoUnit, ecoclass = ecoclass)
        fname = os.path.basename(link)
        out_path = os.path.join(out_dir, fname)
        if aux:
            r = send_request(link = link, path = out_path, save = save)
        else:
            save_mod = l in ['{ecoclass}.json', '{ecoclass}.pdf']
            r = send_request(link = link, path = out_path, save = save_mod)
        if l == '{ecoclass}/states.json':
            if r:
                if r.status_code != 404:
                    state_list = r.json()
    return state_list


def get_community(community, state, landUse, ecoclass, geoUnit, path, catalog = 'esd', save = True):
    prod_list = None
    base = (f'{BASE_URL}/services/plant-community-tables/'
            '{catalog}/{geoUnit}/{ecoclass}/')
    links = ['{landUse}/{state}/{community}/annual-production.json']
    add_links = ['{landUse}/{state}/{community}/canopy-structure.json',
                 '{landUse}/{state}/{community}/forest-overstory.json',
                 '{landUse}/{state}/{community}/forest-understory.json',
                 '{landUse}/{state}/{community}/ground-cover.json',
                 '{landUse}/{state}/{community}/rangeland-plant-composition.json',
                 '{landUse}/{state}/{community}/snag-count.json',
                 '{landUse}/{state}/{community}/soil-surface-cover.json',
                 '{landUse}/{state}/{community}/woody-ground-cover.json'
                ]
    if save:
        links.extend(add_links)
    com_dir = '_'.join((str(landUse), str(state), str(community)))
    out_dir = os.path.join(path, catalog, geoUnit, ecoclass, com_dir)
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    for l in links:
        l_full = ''.join((base, l))
        link = l_full.format(catalog = catalog, geoUnit = geoUnit, ecoclass = ecoclass,
                             landUse = landUse, state = state, community = community)
        fname = os.path.basename(link)
        out_path = os.path.join(out_dir, fname)
        r = send_request(link = link, path = out_path, save = save)
        if l == '{landUse}/{state}/{community}/annual-production.json':
            if r:
                if r.status_code != 404:
                    prod_list = r.json()

    return prod_list


def download_edit(path, geounits, catalog_aux = False, geounit_aux = True, ecoclass_aux = True,
                  eco_save = True, state_save = True, start_at = None):
    gu_dict = get_catalog(path=path, catalog='esd', save=catalog_aux)
    gu_list = [x.get('symbol') for x in gu_dict.get('geoUnits')]
    for g in geounits:
        if g not in gu_list:
            print("Could not find", g, "in available geoUnits.")
        else:
            print('Downloading ', g, '...', sep = '')
            class_dict = get_geoUnit(geoUnit=g, path=path, catalog='esd', save=geounit_aux)
            class_list = [x.get('id') for x in class_dict.get('ecoclasses')]
            if start_at is not None:
                start_idx_list = [class_list.index(x) for x in start_at if x in class_list]
                if start_idx_list:
                    start_idx = max(start_idx_list)
                else:
                    start_idx = 0
            else:
                start_idx = 0
            for ecoclass in class_list[start_idx:]:
                print('\t', ecoclass, '...', sep='')
                state_dict = get_ecoclass(ecoclass=ecoclass, geoUnit=g, path=path, catalog='esd',
                                          save=eco_save, aux=ecoclass_aux)
                if state_save:
                    state_list = [{'landUse': x.get('landUse'), 'state':x.get('state'),
                                   'community': x.get('community')} for x in 
                                  state_dict.get('states') if x.get('community') != 'NA']
                    for sdict in state_list:
                        print('\t\tState: ', sdict, sep='')
                        prod_dict = get_community(community=sdict.get('community'),
                                                  state=sdict.get('state'),
                                                  landUse=sdict.get('landUse'),
                                                  ecoclass=ecoclass, geoUnit=g,
                                                  path=path, catalog='esd', save=state_save)





if __name__ == "__main__":
    argv = sys.argv[1:]

    parser = argparse.ArgumentParser(
            description='Bulk download data from EDIT. Downloads PDF/JSON for each ecological '
                        'site to its own folder within a catalog/geounit parent folder '
                        'which mirrors the source data structure ({catalog}/{geoUnit}/{ecoclass}) '
                        'e.g. outpath/esd/010X/R010XA001OR/*. '
                        'Additional parameters may be passed to the script to download additional '
                        'data.')
    parser.add_argument('outpath', help='path where the exports will be saved')
    parser.add_argument('-c', '--catalog_aux', action = 'store_true',
                        help='Download catalog level data (e.g. global geounit lists)')
    parser.add_argument('-u', '--geounit_aux', action = 'store_true',
                        help='Download associated data for entire geoUnit (e.g. compiled tabular '
                             'data, keys, etc.')
    parser.add_argument('-e', '--ecoclass_aux', action = 'store_true',
                        help='Download auxiliary data associated for an ecoclass (e.g. individual '
                             'subsections of the main JSON, also in JSON format.')
    parser.add_argument('-s', '--states', action = 'store_true',
                        help='Download state and community composition data (stored in subfolders '
                             ' in the format of {landUse #}_{state #}_{community #})')
    parser.add_argument('-g', '--geounits', nargs = '*',
                        help = r'An set of MLRA/LRU codes in the format of "\d{3}[A-Z]" (e.g. '
                               '"010X") whose data will be downloaded to `outpath`')
    parser.add_argument('-a', '--start_at', nargs = '*',
                        help = 'An set of ecosite codes in the format of '
                               r'"[A-Z]{1,2}\d{3}[A-Z]{2}\d{3}[A-Z]{2}" (e.g. "R028AY017NV") which '
                               'will serve as the starting point in a download list. Useful for '
                               'restarting a download which was incomplete due to a timeout or '
                               'other reason.')
    parser.add_argument('-t', '--timeout', type = int,
                        help = 'The number of seconds to wait on a connection before a timeout '
                               'error.')

    args = parser.parse_args(argv)

    if args.timeout:
        TIMEOUT = args.timeout

    download_edit(path=args.outpath, geounits=args.geounits, catalog_aux=args.catalog_aux,
                  geounit_aux=args.geounit_aux, ecoclass_aux=args.ecoclass_aux,
                  state_save=args.states, start_at = args.start_at)

    print('\nScript finished.\n')
