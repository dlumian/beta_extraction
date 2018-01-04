#!~/anaconda2/bin/python
"""
Created on Thu Dec 10 14:25:26 2015

Beta extraction for AFNI Datasets
    -Will need to edit 
        +ds path (wildcards accepted)
        +Subbricks and labels

@author: kudu 
"""
## get python tools
import subprocess
import glob
import datetime
import pandas as pd

#Command to execute shell commands
def shell(cmd):
    try:
        output = subprocess.check_output(cmd, shell=True)
    except Exception:
        print Exception
        output = 'error none'
    finished = output.split(' ')
    print output
    return finished
#command to run pulls
def pull_betas(row, mask, sb_num, m_num, s_num):
    cmd="3dmaskave -mask '" + mask + "["  + str(sb_num) + "]<"+ str(m_num) + ">'  " + row.path + '[' + str(s_num) + ']'
    print cmd
    out=shell(cmd)
    return out[0]
    
#### find datasets 
ds = glob.glob('/data1/psychology/mcraelab/studies/SAA/saa_*/M803*/Study*/recon/2017_saa_acq_out_fix_el/stats.s*+tlrc.HEAD')

# set up dataframe
df = pd.DataFrame()
df['path'] = ds
df['id'] = df.apply(lambda ser: ser['path'].split('/')[7], axis=1)

### select SUBJECT subbriks to pull
subbriks = [17, 20, 23, \
            26, 29, 32, \
            35, 38, 41, \
            44, 47, 50, \
            53, 56, 59, \
            62, 83, 86, \
            89, 92, 95, \
            98, 101, 104, \
            107, 110, 113, \
            116, 119, 122, \
            125, 128, 131, \
            134, 137, 140, \
            143, 146, 149, \
            152, 155, 158, \
            161, 164, 167, \
            170, 173]
### labels for SUBJECT subriks to pull
sb_labs = ['CS_plus_P_nr_1', 'CS_plus_P_nr_2', 'CS_plus_N_nr_1',\
           'CS_plus_N_nr_2', 'CS_minus_P_1', 'CS_minus_P_2',\
           'CS_minus_N_1', 'CS_minus_N_2', 'CS_plus_P_rein_1', \
           'CS_plus_P_rein_2', 'CS_plus_N_rein_1', 'CS_plus_N_rein_2', \
           'fix_rein_1', 'fix_rein_2', 'fix_nr_1', \
           'fix_nr_2', 'nr_plus_gt_minus_GLT', 'nr_plus_gt_minus_early_GLT', \
           'nr_plus_gt_minus_late_GLT', 'Positive_CS_plus_nr_gt_CS_minus_GLT', 'Positive_CS_plus_nr_gt_CS_minus_early_GLT',\
           'Positive_CS_plus_nr_gt_CS_minus_late_GLT', 'Neutral_CS_plus_nr_gt_CS_minus_GLT', 'Neutral_CS_plus_nr_gt_CS_minus_early_GLT', \
           'Neutral_CS_plus_nr_gt_CS_minus_late_GLT', 'CS_minus_Positive_gt_neutral_GLT', 'CS_minus_Positive_gt_neutral_early_GLT', \
           'CS_minus_Positive_gt_neutral_late_GLT', 'CS_plus_P_nr_gt_CS_plus_N_nr_GLT', 'CS_plus_P_nr_gt_CS_plus_N_nr_early_GLT', \
           'CS_plus_P_nr_gt_CS_plus_N_nr_late_GLT', 'add_CS_plus_P_nr_sub_CS_plus_N_nr_sub_CS_minus_P_add_CS_minus_N', 'add_CS_plus_P_nr_sub_CS_plus_N_nr_sub_CS_minus_P_add_CS_minus_N', \
           'add_CS_plus_P_nr_sub_CS_plus_N_nr_sub_CS_minus_P_add_CS_minus_N', 'CS_plus_P_rein_gt_CS_plus_P_GLT', 'CS_plus_P_rein_gt_CS_plus_P_early_GLT', \
           'CS_plus_P_rein_gt_CS_plus_P_late_GLT', 'CS_plus_N_rein_gt_CS_minus_N_GLT', 'CS_plus_N_rein_gt_CS_minus_N_early_GLT',\
           'CS_plus_N_rein_gt_CS_minus_N_late_GLT', 'CS_plus_P_rein_gt_CS_plus_P_nr_GLT', 'CS_plus_P_rein_gt_CS_plus_P_nr_early_GLT', \
           'CS_plus_P_rein_gt_CS_plus_P_nr_late_GLT', 'CS_plus_N_rein_gt_CS_plus_N_nr_GLT', 'CS_plus_N_rein_gt_CS_plus_N_nr_early_GLT', \
           'CS_plus_N_rein_gt_CS_plus_N_nr_late_GLT', 'CS_plus_nr_1_gt_CS_plus_nr_2_GLT']
### pair nums and labels (will fail if len !=)
paired=zip(subbriks, sb_labs)

#### READ IN CSV File of MASKS w/ 3 cols: MASK, SUBBRIK_NUM, LABEL
m_info = pd.read_csv('afni_beta_extract_masks.csv')

### start main loop for beta extract-iterate over masks
for i, r in m_info.iterrows():
    for num, label2 in paired:
        name = r.LABEL + '_' + label2
        df[name]=df.apply(pull_betas, axis=1, args=[r.MASK, r.SUBBRIK, r.VALUES, num])
    
### output infor will timestamp file
i=datetime.datetime.now()
date="%s.%s.%s" % (i.month, i.day, i.year)
of = 'SAA_ACQ_EL_betas_' + date +'.csv'
df.to_csv(of, index=False)
