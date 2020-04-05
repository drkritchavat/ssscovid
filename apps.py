import json
from mysql import mysql_engine
import pandas as pd
import datetime
import paramiko
import sys
from objs import cols, pk_cols, opd_cols, ipd_cols
from mysql import mysql_engine

td = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
f = open('/home/voravit/ssscovid/config/datetime.txt','r')
try:
    startdate = sys.argv[2]
except:
    startdate = f.read()
f.close()
print(f'Starting at {td}')

cbdc = mysql_engine(sys.argv[1])
cbdcEng = cbdc.create_engine()

enddate = td
query = f"""
    SELECT 
        * 
    FROM tbl_hospital_ddcsss 
    WHERE
        RECORD_DATE > '{startdate}' AND
        RECORD_DATE <= '{enddate}'
    """
print(query,cbdc)
df = pd.read_sql_query(query,cbdcEng)
ndf = len(df)
if ndf > 0:
    transformed_df = df[cols]\
                        .assign(
                            OPD_ICD10 = lambda x: x.loc[:,opd_cols[1:-1]].values.tolist(),
                            IPD_ICD10 = lambda x: x.loc[:,ipd_cols[2:-2]].values.tolist(),
                            OPD_RESULT = lambda x: x.loc[:,['OPD_RESULT']].values.tolist(),
                            IPD_RESULT = lambda x: x.loc[:,['IPD_DISCHARGE_STATUS','IPD_DISCHARGE_TYPE']].values.tolist(),
                            type = lambda x: x['AN'].notnull() + 1
                        )\
                        .drop(columns = opd_cols[1:-1]+ipd_cols[2:-2]+['AN','IPD_DISCHARGE_STATUS','IPD_DISCHARGE_TYPE'])\
                        .set_index(['type']+pk_cols)

    stacked_cols = ['DATEDEFINE','ICD10','RESULT']
    opd_df = transformed_df\
                .loc[1]\
                .loc[:,['OPD_VISIT_DATE','OPD_ICD10','OPD_RESULT']]\
                .assign(type = 1)\
                .set_index('type',append=True)
    opd_df.columns = stacked_cols

    ipd_df = transformed_df\
                .loc[2]\
                .loc[:,['IPD_REGIST_DATE','IPD_ICD10','IPD_RESULT']]\
                .assign(type = 2)\
                .set_index('type',append=True)
    ipd_df.columns = stacked_cols

    stack_df = pd.concat([opd_df,ipd_df])\
                .reset_index()\
                .assign(DATEDEFINE = lambda x:pd.to_datetime(x['DATEDEFINE']).dt.date)\
                .groupby([
                    'HOSP_CODE',
                    'HN',
                    'PATIENT_LOCATION_CODE',
                    'type',
                    'DATEDEFINE'
                ])\
                .apply(lambda x: {
                    'ICD10':[i for i in x['ICD10']],
                    'RESULT':[i for i in x['RESULT']]
                })\

    result_df = pd.concat([
                    stack_df.map(lambda x: x['ICD10']).rename('ICD10'),
                    stack_df.map(lambda x: x['RESULT']).rename('RESULT')
                    ],axis=1)\
                .reset_index()\
                .sort_values('DATEDEFINE')

    csv_df = result_df.to_csv(index=False)
    HOS_CODE = df['HOSP_CODE'].iloc[0]
    YYYYMMDD = datetime.datetime.now().strftime('%Y-%m-%d')
else:
    csv_df = "None"
    YYYYMMDD = datetime.datetime.now().strftime('%Y-%m-%d')
    HOS_CODE = "None"
f = open('/home/voravit/ssscovid/config/datetime.txt','w')
f.write(td)
f.close()


"""
SSH
"""
ssh = paramiko.SSHClient()
ssh.load_host_keys('/home/voravit/.ssh/known_hosts') 
ssh.connect('covid-ddc2.bigstream.cloud', username='sss')
sftp = ssh.open_sftp()
with sftp.open(f'/home/sss/visits/{YYYYMMDD}-{HOS_CODE}.csv','w') as f:
    f.write(csv_df)
sftp.close()
ssh.close()
