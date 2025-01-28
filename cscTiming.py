#imports here
import os
import numpy as np
import pandas as pd
import uproot
import ROOT
import runregistry
from multiprocessing.pool import ThreadPool
import cernrequests
import argparse



def main(run_nums, ls, rc):
    """
    run numbers to be queried, type of plots to show, run class, lumisections
    """
    # print('Searching for plot types: ')
    # print(plot_types)

    TIMEOUT = 500 # keep relatively high, especially for large sets

    def _parse_run_full_name(full_name):
        """
        returns the simplified form of a full DQM name.
        """
        if VERBOSE >= 2:
            print(f'\ndqm.py _parse_run_full_name(full_name = {full_name})')

        if full_name.split('_')[2].startswith('R000'):          # OfflineData
            name = full_name.split('_')[2][1:]
            return str(int(name)) # why both??
        elif full_name.split('_')[3].startswith('R000'):        # OnlineData
            name = full_name.split('_')[3][1:].replace('.root','')
            return str(int(name))
        else:
            raise ValueError('dqm.py _pars_run_full_name({}), failed to parse run number!'.format(full_name))



    runs = [run.strip() for run in run_nums.split(',')] # splits runs by commas, removes extra spaces

    new_runs = []
    for run in runs:
        if ':' in run:                  # allows run range to be entered in xxxxxx:xxxxxx format
            bounds = run.split(':')
            for new_run in range(int(bounds[0]),int(bounds[1])+1):
                new_runs.append(str(new_run))
        else:
            new_runs.append(run)        # otherwise just adds runs, split by commas in previous step
    runs = new_runs                     ## runs is now list of run nums as strings

    runs_int = [int(run) for run in runs]

    print('Run class (Collisions or Cosmics/Comissioning): ')
    if rc == 'Collisions':
        print(rc)
        request = runregistry.get_runs(
                filter = {
                    'class':{
                        'or':[
                            'Collisions25',
                            'Collisions24',
                            'Collisions23',
                            'Collisions22',
                            'Collisions18'
                        ]
                    },
                    'run_number':{
                        'and':[
                            {'>=': min(runs_int)},
                            {'<=': max(runs_int)}
                        ]
                    }
                }
        )
    elif rc == 'CollisionsHI':
        print(rc)
        request = runregistry.get_runs(
                filter = {
                    'class':{
                        'or':[
                            'Collisions25HI',
                            'Collisions24HI'
                        ]
                    },
                    'run_number':{
                        'and':[
                            {'>=': min(runs_int)},
                            {'<=': max(runs_int)}
                        ]
                    }
                }
        )
    
    else:
        print(rc) # might as well
        request = runregistry.get_runs(
                filter = {
                    'class':{
                        'or':[
                            'Cosmics25',
                            'Cosmics24',
                            'Cosmics23',
                            'Cosmics22',
                            'Cosmics18',
                            'Commissioning25'
                            'Commissioning24'
                            'Commissioning23'
                            'Commissioning22'
                            'Commissioning'
                        ]
                    },
                    'run_number':{
                        'and':[
                            {'>=': min(runs_int)},
                            {'<=': max(runs_int)}
                        ]
                    }
                }
        )

    if ls == '':
        ls = 1
        print(f'The value of ls is {ls}')

    min_ls_duration = int(ls)
    valid_runs = []
    valid_dates = []
    for run in request:             # ensures only runs above (or = to) min ls duration are chosen
        if int(run['oms_attributes']['ls_duration']) < min_ls_duration:
            continue
        valid_runs += [str(run['oms_attributes']['run_number'])]
        valid_dates += [str(run['oms_attributes']['start_time'])[5:10]]


    new_runs = []
    dates = []
    for run in runs:
        try:                            # will go through all runs, find indices of valid ones, and add those runs to new lists
            i = valid_runs.index(run)
            new_runs.append(valid_runs[i])
            dates.append(valid_dates[i])
        except:
            print('Skipping run:',str(run))

    runs = new_runs
    print('Valid runs: ')
    print(runs)

    file_names = []
    for run in runs:
        file_names.append(f'https://cmsweb.cern.ch/dqm/offline/data/browse/ROOT/OnlineData/original/000{run[:2]}xxxx/000{run[:4]}xx/DQM_V0001_L1T_R000{run}.root')

    full_path = 'https://cmsweb.cern.ch/dqm/offline/data/browse/ROOT/OfflineData/Run2024/ZeroBias/0003849xx/DQM_V0001_R000384946__ZeroBias__Run2024G-PromptReco-v1__DQMIO.root'
    response = cernrequests.get(full_path, stream=True, timeout=500)
    print("response status code: %d" % response.status_code)
    with open(f'tmp.root', 'wb') as f:
        for chunk in response.iter_content(chunk_size=403):
            f.write(chunk)

    file = uproot.open('./tmp.root')

    hits_bx0 = file["DQMData/Run 384946/L1T/Run summary/L1TStage2EMTF/Timing/cscLCTTimingBX0;1"].values()
    hits_bxneg1 = file["DQMData/Run 384946/L1T/Run summary/L1TStage2EMTF/Timing/cscLCTTimingBXNeg1;1"].values()
    hits_bxpos1 = file["DQMData/Run 384946/L1T/Run summary/L1TStage2EMTF/Timing/cscLCTTimingBXPos1;1"].values()
    del file
    os.remove('./tmp.root')
    hits_bx0 = np.zeros(hits_bx0.shape)
    hits_bxneg1 = np.zeros(hits_bxneg1.shape)
    hits_bxpos1 = np.zeros(hits_bxpos1.shape)
    plots = []
    final_runs = []

    def process_file(idx, fn):
        nonlocal final_runs
        cert = cernrequests.certs.default_user_certificate_paths()
        ca_bundle = cernrequests.certs.where()
        response = cernrequests.get(fn, stream=True, timeout=500)

        with open(f'./tmp{idx}.root', 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                #print(chunk)
                if response.status_code == 404:
                    print("Error: file not found")
                    print("The filename is " + str(fn))
                    break
                f.write(chunk)
        del response

        try:
            nonlocal hits_bx0
            nonlocal hits_bxneg1
            nonlocal hits_bxpos1
            run_num = runs[idx]
            print(f'Run: {run_num}')
            file = uproot.open(f'./tmp{idx}.root')
            hits_bx0 += file[f'DQMData/Run {run_num}/L1T/Run summary/L1TStage2EMTF/Timing/cscLCTTimingBX0;1'].values()
            hits_bxneg1 += file[f'DQMData/Run {run_num}/L1T/Run summary/L1TStage2EMTF/Timing/cscLCTTimingBXNeg1;1'].values()
            hits_bxpos1 += file[f'DQMData/Run {run_num}/L1T/Run summary/L1TStage2EMTF/Timing/cscLCTTimingBXPos1;1'].values()
            del file
            file = ROOT.TFile(f'./tmp{idx}.root')
            del file
            final_runs += [runs[idx]]
        except Exception as e: print(e)

        os.remove(f'./tmp{idx}.root')

    pool = ThreadPool().imap_unordered(lambda p: process_file(*p), enumerate(file_names))

    for result in pool:
        print(result)

    # Small fix for writting to persistent file
    outfile = ROOT.TFile('./data.root', 'recreate')

    outfile.Close() # as per osvaldo's suggestions
    del outfile

    print(f'The final runs are {final_runs}')

    # this can definitely be cleaned up but i'm glad it's fully written out for my sake lol
    hits_bx0_noneighbors = np.delete(hits_bx0, [2, 9, 16, 23, 30, 37], 0)
    hits_bxneg1_noneighbors = np.delete(hits_bxneg1, [2, 9, 16, 23, 30, 37], 0)
    hits_bxpos1_noneighbors = np.delete(hits_bxpos1, [2, 9, 16, 23, 30, 37], 0)

    arr_hits_bx0 = np.reshape(hits_bx0_noneighbors, 720, order='F')
    arr_hits_bxneg1 = np.reshape(hits_bxneg1_noneighbors, 720, order='F')
    arr_hits_bxpos1 = np.reshape(hits_bxpos1_noneighbors, 720, order='F')


    station_ring = ['ME-4/2','ME-4/1','ME-3/2','ME-3/1','ME-2/2','ME-2/1','ME-1/3','ME-1/2','ME-1/1b','ME-1/1a','ME+1/1a','ME+1/1b','ME+1/2','ME+1/3','ME+2/1','ME+2/2','ME+3/1','ME+3/2','ME+4/1','ME+4/2']
    inner_station_ring = ['ME-4/1','ME-3/1','ME-2/1','ME+2/1','ME+3/1','ME+4/1']
    clist_ints = []
    for x in range(1,37): clist_ints.append(x)
    chamber = [str(f) for f in clist_ints]

    all_names = []

    for idx_station_ring, station_ring_name in enumerate(station_ring):
        for idx_chamber, chamber_number in enumerate(chamber):
            if station_ring_name in inner_station_ring:
                half_chamber_number = str(int(chamber_number)/2)
                new_name = station_ring_name + '/' + half_chamber_number
            else:
                new_name = station_ring_name + '/' + chamber_number
            all_names.append(new_name)


    df = pd.DataFrame({'Chamber': all_names,
                       'BX-1': arr_hits_bxneg1,
                       'BX0': arr_hits_bx0,
                       'BX+1':arr_hits_bxpos1})

    df_drop_half = (df
            .assign(has_half = lambda x: x['Chamber'].str.contains('\.5'),
                    a_or_b = lambda x: x['Chamber'].str.contains('a') | x['Chamber'].str.contains('b'))
            .query('(~has_half) & (~a_or_b)')
            .drop(['has_half', 'a_or_b'], axis=1))

    subset = (df_drop_half
            .assign(has_point = lambda x: x['Chamber'].str.contains('\.'))
            .query('has_point')
            .assign(Chamber = lambda x: x['Chamber'].str.replace('\.0',''))
            .assign(new_bx1 = lambda x: 2 * x['BX-1'],
                    new_bx0 = lambda x: 2 * x['BX0'],
                    new_bxp1 = lambda x: 2 * x['BX+1'])
            [['Chamber','new_bx1','new_bx0','new_bxp1']]
            .rename({'new_bx1': 'BX-1','new_bx0':'BX0','new_bxp1':'BX+1'}, axis=1))

    df_drop_half.loc[subset.index] = subset


    #create df with ME1/1a chambers
    df1 = df[df['Chamber'].str.contains('a')]
    #get names of ME1 chambers
    me1_names = df1['Chamber'].str.replace('a','',regex=True)
    #create df with ME1/1b chambers
    df2 = df[df['Chamber'].str.contains('b')]

    # combine ME1/1a and ME1/1b chambers
    me1_bx0 = df1['BX0'].to_numpy() + df2['BX0'].to_numpy()
    me1_bxneg1 = df1['BX-1'].to_numpy() + df2['BX-1'].to_numpy()
    me1_bxpos1 = df1['BX+1'].to_numpy() + df2['BX+1'].to_numpy()

    df_me1 = pd.DataFrame({'Chamber': me1_names,
                           'BX-1': me1_bxneg1,
                           'BX0': me1_bx0,
                           'BX+1': me1_bxpos1})

    df_final = pd.concat([df_drop_half,df_me1])
    pd.set_option('display.max_rows', 1000)
    # df_final.sort_index()
    del df_drop_half
    del df_me1
    print(df_final)
    df_final.to_csv('cscTiming.csv', index=False)
    os.remove('data.root')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Arguments for CSC Timing tables')
    parser.add_argument('--runs', required=True,
                        help='list of run numbers separated by comma or range separated by colon')
    parser.add_argument('--minLS', required=True,
                        help='minimum LS duration of the runs')
    parser.add_argument('--runClass', required=True,
                        help='Run class in runregistry. Could be Collisions, CollisionsHI, Cosmics, or Commissioning')

    args = parser.parse_args()
    main(run_nums=args.runs, ls=args.minLS, rc=args.runClass)