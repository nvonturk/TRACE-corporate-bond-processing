##########################################
# Trace Standard processing              #
# Nick von Turkovich                     #
# Email: nvonturk@mit.edu                #
# Date: July 2024                        #
# Updated:  July 2024                    #
# Version:  1.0.0                        #
##########################################

##########################################
# I ackowledge                           #
# Qingyi (Freda) Song Drechsler          #
# for writing similar SAS code available #
# on the WRDS Bond Returns Module        #
# This code translates large portions of #
# this SAS code to Python                #
##########################################

import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'
import numpy as np
import wrds
import sys
import os

valid_agg_levels = ['daily', 'hourly']
valid_feed_types = ['non_rule_144a', 'rule_144a']

def parse_arguments():
    args = sys.argv[1:]

    if len(args) > 3:
        raise ValueError('Too many arguments')
    elif len(args) == 3:
        if not os.path.isfile(args[0]):
             raise ValueError('First argument must be a valid file path to CUSIP IDs')
        if args[0] not in valid_agg_levels:
            raise ValueError('First argument must be either "daily" or "hourly"')
        if args[1] not in valid_feed_types:
            raise ValueError('Second argument must be either "non_rule_144a" or "rule_144a')
        ids_filepath = args[0]
        agg_level = args[1]
        feed_type = args[2]
    elif len(args) == 2:
        if not os.path.isfile(args[0]):
             raise ValueError('First argument must be a valid file path to CUSIP IDs')
        if args[0] not in valid_agg_levels:
            raise ValueError('First argument must be either "daily" or "hourly"')
        ids_filepath = args[0]
        agg_level = args[1]
    elif len(args) == 1:
        if not os.path.isfile(args[0]):
             raise ValueError('First argument must be a valid file path to CUSIP IDs')
        ids_filepath = args[0]
        agg_level = 'daily'
        feed_type = 'non_rule_144a'
    else:
        ids_filepath = 'IDs.csv'
        agg_level = 'daily'
        feed_type = 'non_rule_144a'
    
    return ids_filepath, agg_level, feed_type

def gather_trace_data(db, cusip_chunk, feed_type):
    parm = {'cusip_id': (tuple(cusip_chunk))}
    
    trace_btds = db.raw_sql('SELECT * FROM trace_standard.trace WHERE cusip_id in %(cusip_id)s', params=parm)
    trace_btds144a = db.raw_sql('SELECT * FROM trace_standard.trace_btds144a WHERE cusip_id in %(cusip_id)s', params=parm)

    trace_btds['trans_dt'] = pd.to_datetime(trace_btds['trans_dt'], format = '%Y-%m-%d')
    trace_btds144a['trans_dt'] = pd.to_datetime(trace_btds144a['trans_dt'], format = '%Y-%m-%d')

    if feed_type == 'rule_144a':
        if len(trace_btds[trace_btds['trans_dt'] >= '2014-06-30']) > 0:
            raise ValueError('BTDS feed has data after June 30, 2014')
        
        if len(trace_btds144a[trace_btds144a['trans_dt'] < '2014-06-30']) > 0:
            raise ValueError('BTDS144a feed has data before June 30, 2014')

        trace_btds = trace_btds[~(trace_btds['trans_dt'] >= '2014-06-30')]
        trace_btds144a = trace_btds144a[trace_btds144a['trans_dt'] >= '2014-06-30']
        trace = pd.concat([trace_btds, trace_btds144a], ignore_index=True)
    else:
        trace = trace_btds
    
    return trace

def clean_trace_data(trace):
            
    trace['trd_exctn_dt'] = pd.to_datetime(trace['trd_exctn_dt'], format = '%Y-%m-%d')    
    
    trace['days_to_sttl_ct'] = trace['days_to_sttl_ct'].astype('str')                   
    trace['wis_fl'] = trace['wis_fl'].astype('str')     
    trace['sale_cndtn_cd'] = trace['sale_cndtn_cd'].astype('str') 
    trace['ascii_rptd_vol_tx'] = trace['ascii_rptd_vol_tx'].replace({'5MM+': '5000000', '1MM+': '1000000'})
    trace['entrd_vol_qt'] = pd.to_numeric(trace['ascii_rptd_vol_tx'], errors='coerce')

    _clean1 = trace[trace['cusip_id'].isnull() == False]
    _clean1['trc_st'] = _clean1['trc_st'].replace({'G': 'T', 'M': 'T', 'H': 'C', 'N': 'C', 'I': 'W', 'O': 'W'})
    columns_to_keep = [
        'cusip_id', 'bond_sym_id', 'bsym', 'trd_exctn_dt', 'trd_exctn_tm', 'msg_seq_nb', 'trc_st', 'wis_fl', 'cmsn_trd',
        'entrd_vol_qt', 'rptd_pr', 'yld_pt', 'asof_cd', 'side', 'diss_rptg_side_cd', 'orig_msg_seq_nb', 'orig_dis_dt',
        'rptg_party_type', 'contra_party_type'
    ]
    _clean1 = _clean1[columns_to_keep].rename(columns={'diss_rptg_side_cd': 'rpt_side_cd'})

    _c = _clean1[_clean1['trc_st'] == 'C']
    _w = _clean1[_clean1['trc_st'] == 'W']
    _t = _clean1[_clean1['trc_st'] == 'T']                        

    _clean2 = pd.merge(
        _t, _c[['cusip_id', 'trd_exctn_dt', 'trd_exctn_tm', 'rptd_pr', 'entrd_vol_qt', 'orig_msg_seq_nb', 'trc_st']],
        how='left',
        left_on=['cusip_id', 'trd_exctn_dt', 'trd_exctn_tm', 'rptd_pr', 'entrd_vol_qt', 'msg_seq_nb'],
        right_on=['cusip_id', 'trd_exctn_dt', 'trd_exctn_tm', 'rptd_pr', 'entrd_vol_qt', 'orig_msg_seq_nb'],
        suffixes=('', '_c')
        ).drop(columns=['orig_msg_seq_nb_c']).drop_duplicates()
    
    _del_c = _clean2[_clean2['trc_st_c'] == 'C']
    _clean2 = _clean2[_clean2['trc_st_c'].isnull()].drop(columns=['trc_st_c'])

    #* ************************************ */
    #* 3.0 Removing Correction Cases        */
    #* ************************************ */ 
    
    # NOTE: on a given day, a bond can have more than one round of correction. One W to correct an older W, which then corrects the original T. Before joining back to the T data, first need to clean out the W to handle the situation described above. The following section handles the chain of W cases.

    # 3.1 Sort out all msg_seq_nb
    __w_msg = _w[['cusip_id', 'bond_sym_id', 'trd_exctn_dt', 'trd_exctn_tm', 'msg_seq_nb']]
    __w_msg['flag'] = 'msg'

    # Sort out all mapped original msg_seq_nb
    __w_omsg = _w[['cusip_id', 'bond_sym_id', 'trd_exctn_dt', 'trd_exctn_tm', 'orig_msg_seq_nb']]
    __w_omsg['flag'] = 'omsg'
    __w_omsg = __w_omsg.rename(columns={'orig_msg_seq_nb': 'msg_seq_nb'})

    __w = pd.concat([__w_omsg, __w_msg])

    # 3.2 Count the number of appearance (napp) of a msg_seq_nb: if appears more than once then it is part of later correction
    __w_napp = __w.groupby(['cusip_id', 'bond_sym_id', 'trd_exctn_dt', 'trd_exctn_tm', 'msg_seq_nb']).size().reset_index(name='napp')

    # 3.3 Check whether one msg_seq_nb is associated with both msg and orig_msg or only to orig_msg. If msg_seq_nb appearing more than once is associated with only orig_msg then it means that more than one msg_seq_nb is linked to the same orig_msg_seq_nb for correction. Examples: cusip_id='362320AX1' and trd_Exctn_dt='04FEB2005'd (3 cases like this in total). If ntype=2 then a msg_seq_nb is associated with being both msg_seq_nb and orig_msg_seq_nb.
    __w_mult = __w[['cusip_id', 'bond_sym_id', 'trd_exctn_dt', 'trd_exctn_tm', 'msg_seq_nb', 'flag']].drop_duplicates()
    __w_mult1 = __w_mult.groupby(['cusip_id', 'bond_sym_id', 'trd_exctn_dt', 'trd_exctn_tm', 'msg_seq_nb']).size().reset_index(name='ntype').drop_duplicates()

    # 3.4 Combine the npair and ntype info
    __w_comb = pd.merge(__w_napp, __w_mult1[["cusip_id", "trd_exctn_dt", "trd_exctn_tm", "msg_seq_nb", "ntype"]], on=['cusip_id', 'trd_exctn_dt', 'trd_exctn_tm', 'msg_seq_nb'], how='left').sort_values(by=['cusip_id', 'trd_exctn_dt', 'trd_exctn_tm']).drop_duplicates()

    # Map back by matching CUSIP Excution Date and Time to remove msg_seq_nb that appears more than once. If napp=1 or (napp>1 but ntype=1)
    __w_keep = pd.merge(__w_comb[(__w_comb['napp'] == 1) | ((__w_comb['napp'] > 1) & (__w_comb['ntype'] == 1))], __w, on=['cusip_id', 'trd_exctn_dt', 'trd_exctn_tm', 'msg_seq_nb'], how='inner', suffixes=('', '_DROP')).filter(regex='^(?!.*_DROP)').sort_values(by=['cusip_id', 'trd_exctn_dt', 'trd_exctn_tm']).drop_duplicates()

    # 3.5 Caluclate no of pair of records
    __w_keep['npair'] = __w_keep.groupby(by=['cusip_id', 'trd_exctn_dt', 'trd_exctn_tm'])['cusip_id'].transform("count") / 2
    __w_keep = __w_keep.drop_duplicates().sort_values(by=['cusip_id', 'trd_exctn_dt', 'trd_exctn_tm'])

    # For records with only one pair of entry at a given time stamp - transpose using the flag information
    __w_keep1 = __w_keep[__w_keep['npair'] == 1].pivot(index=['cusip_id', 'trd_exctn_dt', 'trd_exctn_tm'], columns='flag', values='msg_seq_nb').reset_index().rename(columns={'msg': 'msg_seq_nb', 'omsg': 'orig_msg_seq_nb'})

    # For records with more than one pair of entry at a given time stamp - join back the original msg_seq_nb
    __w_keep2 = pd.merge(__w_keep[((__w_keep['npair'] > 1) & (__w_keep['flag'] == 'msg'))][['cusip_id', 'bond_sym_id', 'trd_exctn_dt', 'trd_exctn_tm', 'msg_seq_nb']], _w[['cusip_id', 'trd_exctn_dt', 'trd_exctn_tm', 'msg_seq_nb', 'orig_msg_seq_nb']], left_on=['cusip_id', 'trd_exctn_dt', 'trd_exctn_tm', 'msg_seq_nb'], right_on=['cusip_id', 'trd_exctn_dt', 'trd_exctn_tm', 'msg_seq_nb'], how='left').drop_duplicates()

    __w_clean = pd.concat([__w_keep1, __w_keep2], axis=0)
    __w_clean.drop(columns=['bond_sym_id'], inplace=True)

    # 3.6 Join back to get all the other information
    _w_clean = pd.merge(__w_clean, _w.drop(columns=['orig_msg_seq_nb']), left_on=['cusip_id', 'trd_exctn_dt', 'trd_exctn_tm', 'msg_seq_nb'], right_on=['cusip_id', 'trd_exctn_dt', 'trd_exctn_tm', 'msg_seq_nb'], how='left').drop_duplicates()

    # 3.7 Match up with Trade Record data to delete the matched T record; matching by cusip_id, date, and msg_seq_nb; W records show orig_msg_seq_nb matching original record msg_seq_nb
    _clean3 = pd.merge(_clean2, _w_clean[['cusip_id', 'trd_exctn_dt', 'trd_exctn_tm', 'orig_msg_seq_nb', 'msg_seq_nb', 'trc_st']], left_on=['cusip_id', 'trd_exctn_dt', 'msg_seq_nb'], right_on=['cusip_id', 'trd_exctn_dt', 'orig_msg_seq_nb'], how='left', suffixes=('', '_w')).rename(columns={'msg_seq_nb_w': 'mod_msg_seq_nb', 'orig_msg_seq_nb_w': 'mod_orig_msg_seq_nb'}).drop_duplicates()

    _del_w = _clean3[_clean3['trc_st_w'] == 'W']

    # Delete matched T records
    _clean3 = _clean3[_clean3['trc_st_w'].isnull()].drop(columns = ['trc_st_w', 'mod_msg_seq_nb', 'mod_orig_msg_seq_nb', 'trd_exctn_tm_w'])

    # Replace T records with corresponding W records; filter out W records with valid matching T from the previous step
    _rep_w = pd.merge(_w_clean, _del_w[['cusip_id', 'trd_exctn_dt', 'mod_msg_seq_nb', 'mod_orig_msg_seq_nb', 'trc_st_w']], left_on=['cusip_id', 'trd_exctn_dt', 'msg_seq_nb'], right_on=['cusip_id', 'trd_exctn_dt', 'mod_msg_seq_nb'], how='left').drop_duplicates()

    _rep_w = _rep_w[_rep_w['trc_st_w'] == 'W'].drop(columns = ['trc_st_w', 'mod_msg_seq_nb', 'mod_orig_msg_seq_nb'])

    _rep_w = _rep_w.sort_values(by=['cusip_id', 'trd_exctn_dt', 'msg_seq_nb', 'orig_msg_seq_nb', 'rptd_pr', 'entrd_vol_qt']).drop_duplicates(subset = ['cusip_id', 'trd_exctn_dt', 'msg_seq_nb', 'orig_msg_seq_nb', 'rptd_pr', 'entrd_vol_qt'])

    # Combine the cleaned T records and correct replacement W records
    _clean4 = pd.concat([_clean3, _rep_w], axis=0)

    # 4.0 Remove Reversals
    _rev_header = _clean4[(_clean4['asof_cd'] == 'R')][['cusip_id', 'bond_sym_id', 'trd_exctn_dt', 'trd_exctn_tm', 'rptd_pr', 'entrd_vol_qt', 'rpt_side_cd', 'contra_party_type']]

    # Match by only 6 keys: cusip_id, execution date, vol, price, B/S and C/D (remove the time dimension)
    _rev_header_sorted = _rev_header.sort_values(by=['cusip_id', 'bond_sym_id', 'trd_exctn_dt', 'entrd_vol_qt', 'rptd_pr', 'rpt_side_cd', 'contra_party_type', 'trd_exctn_tm'])

    # Reset index for ease of operation
    _rev_header_sorted.reset_index(drop=True, inplace=True)

    # Create a new column to identify groups and reset 'seq' for each group
    group_columns = ['cusip_id', 'bond_sym_id', 'trd_exctn_dt', 'entrd_vol_qt', 'rptd_pr', 'rpt_side_cd', 'contra_party_type']

    # Use groupby to increment seq for each group
    _rev_header_sorted['seq'] = _rev_header_sorted.groupby(group_columns, dropna=False).cumcount() + 1

    _rev_header6 = _rev_header_sorted

    # Create the same ordering among the non-reversal records; remove records that are R (reversal), D (delayed dissemination), and X (delayed reversal)
    _clean5 = _clean4[(_clean4['asof_cd'] != 'R') & (_clean4['asof_cd'] != 'D') & (_clean4['asof_cd'] != 'X')]

    _clean5_header = _clean5[['cusip_id', 'bond_sym_id', 'trd_exctn_dt', 'trd_exctn_tm', 'rptd_pr', 'entrd_vol_qt', 'rpt_side_cd', 'contra_party_type', 'msg_seq_nb']]

    # Match by 6 keys (excluding execution time)
    _clean5_header_sorted = _clean5_header.sort_values(by=['cusip_id', 'bond_sym_id', 'trd_exctn_dt', 'entrd_vol_qt', 'rptd_pr', 'rpt_side_cd', 'contra_party_type', 'trd_exctn_tm', 'msg_seq_nb'])

    # Reset index for ease of operation
    _clean5_header_sorted.reset_index(drop=True, inplace=True)

    # Create a new column to identify groups and reset 'seq6' for each group
    group_columns = ['cusip_id', 'bond_sym_id', 'trd_exctn_dt', 'entrd_vol_qt', 'rptd_pr', 'rpt_side_cd', 'contra_party_type']

    # Use groupby to increment seq6 for each group
    _clean5_header_sorted['seq6'] = _clean5_header_sorted.groupby(group_columns, dropna=False).cumcount() + 1

    _clean5_header = _clean5_header_sorted

    # Join reversal with non-reversal to delete the corresponding ones
    _clean5_header = pd.merge(_clean5_header, _rev_header6.drop(columns=['bond_sym_id', 'trd_exctn_tm']), left_on =['cusip_id', 'trd_exctn_dt', 'entrd_vol_qt', 'rptd_pr', 'rpt_side_cd', 'contra_party_type', 'seq6'], right_on=['cusip_id', 'trd_exctn_dt', 'entrd_vol_qt', 'rptd_pr', 'rpt_side_cd', 'contra_party_type', 'seq'], how='left').rename(columns={'seq': 'rev_seq6'}).drop_duplicates()

    _rev_matched6 = _clean5_header[~(_clean5_header['rev_seq6'].isnull())]

    # As 6 key matching has a higher record of finding reversal match, use the 6 key results now
    _clean5_header = _clean5_header[(_clean5_header['rev_seq6'].isnull())].drop(columns=['rev_seq6'])

    _clean6 = pd.merge(_clean5, _clean5_header[['cusip_id', 'bond_sym_id', 'trd_exctn_dt', 'trd_exctn_tm', 'rptd_pr', 'entrd_vol_qt', 'rpt_side_cd', 'contra_party_type', 'msg_seq_nb']], on=['cusip_id', 'bond_sym_id', 'trd_exctn_dt', 'trd_exctn_tm', 'rptd_pr', 'entrd_vol_qt', 'rpt_side_cd', 'contra_party_type', 'msg_seq_nb'], how='inner').drop_duplicates()

    # 5.0 Clean Agency Transaction

    # New variables available starting in June 2015: SIDE Contra_party_type rptg_party_type
    _clean6['rpt_side_cd'] = _clean6['rpt_side_cd'].combine_first(_clean6['side'])

    # 3.1 Remove trades double reported by both buy and sell of the inter-dealer trade
    _agency_s = _clean6[(_clean6['rpt_side_cd'] == 'S') & (_clean6['contra_party_type'] == 'D')]   

    _agency_b = _clean6[(_clean6['rpt_side_cd'] == 'B') & (_clean6['contra_party_type'] == 'D')]

    trace_standard_clean = _clean6

    return trace_standard_clean


def main():
    ids_filepath, agg_level, feed_type = parse_arguments()
    
    db = wrds.Connection()
    
    IDs = pd.read_csv(ids_filepath)
    
    if feed_type == 'rule_144a':
        IDs = IDs[IDs['rule_144a'] == 'Y']
    else:
        IDs = IDs[IDs['rule_144a'] != 'Y']
    
    CUSIP_Sample = list(IDs['complete_cusip'].unique())
    
    def divide_chunks(l, n):     
        for i in range(0, len(l), n): 
            yield l[i:i + n] 

    cusip_chunks = list(divide_chunks(CUSIP_Sample, 500)) 

    CleaningExport = pd.DataFrame(index=range(0, len(cusip_chunks)), columns=['Obs.Pre', 'Obs.PostBBW', 'Obs.PostDickNielsen'])

    for i, cusip_chunk in enumerate(cusip_chunks):  
        print(i)
        trace = gather_trace_data(db, cusip_chunk, feed_type)

        CleaningExport['Obs.Pre'].iloc[i] = int(len(trace))

        if len(trace) <= 100:
            CleaningExport['Obs.PostBBW'].iloc[i] = int(len(trace))
            CleaningExport['Obs.PostDickNielsen'].iloc[i] = int(len(trace))
            continue
        else:
            trace = clean_trace_data(trace)

            CleaningExport['Obs.PostBBW'].iloc[i] = int(len(trace))
            # trace['trd_exctn_dtm'] = pd.to_datetime(trace['trd_exctn_dt'].astype(str) + trace['trd_exctn_tm'].astype(str), format='%Y-%m-%d%H:%M:%S')

            # trace = trace[trace['entrd_vol_qt'] >= 10000]




            CleaningExport['Obs.PostDickNielsen'].iloc[i] = int(len(trace))

            #* ***************** */
            #* Prices / Volume   */
            #* ***************** */
            # Price - Equal-Weight   #
            

if __name__ == "__main__":
    main()



