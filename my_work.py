import pandas as pd
import datetime
import numpy as np
import icd
def get_age(row):
    """Calculate the age of patient by row
    Arg:
        row: the row of pandas dataframe.
        return the patient age
    """
    raw_age = row['DOD'].year - row['DOB'].year
    if (row['DOD'].month < row['DOB'].month) or ((row['DOD'].month == row['DOB'].month) and (row['DOD'].day < row['DOB'].day)):
        return raw_age - 1
    else:
        return raw_age

mimic_patients = 'mimic_csv/PATIENTS.csv'
mimic_note_events = 'mimic_csv/NOTEEVENTS.csv'
mimic_admissions = 'mimic_csv/ADMISSIONS.csv'
mimic_diagnoses = 'mimic_csv/DIAGNOSES_ICD.csv'

patient = pd.read_csv(mimic_patients)
patient['DOD'] = pd.to_datetime(patient['DOD']).dt.date
patient['DOB'] = pd.to_datetime(patient['DOB']).dt.date
patient['DOD_HOSP'] = pd.to_datetime(patient['DOD_HOSP'])
patient['age'] = patient.apply(get_age, axis=1)

#for col in patient.columns:
#    print(col)  #desc col headers
patient = patient.drop(['DOD_SSN', 'DOD_HOSP'], axis=1)

admission = pd.read_csv(mimic_admissions)
# caculate the admission time for each patient
admission['admit_times'] = admission.groupby(['SUBJECT_ID'])['SUBJECT_ID'].transform('size')

# remove the patient with multiple admissions
admission = admission[admission['admit_times'] > 3]
# drop the column that are not used in the future
#admission = admission.drop(['ROW_ID', 'HADM_ID', 'ADMISSION_TYPE',
#                            'ADMISSION_LOCATION', 'DISCHARGE_LOCATION',
#                            'INSURANCE', 'LANGUAGE', 'RELIGION', 'MARITAL_STATUS',
#                            'ETHNICITY', 'EDREGTIME', 'EDOUTTIME',
#                            'DIAGNOSIS', 'HAS_CHARTEVENTS_DATA', 'admit_times','DEATHTIME'], axis=1)
admission = admission.drop(['DEATHTIME'], axis=1)
patient_filter = pd.merge(patient, admission, on='SUBJECT_ID', how='inner')
patient_filter.shape
patient_filter.head()
pd.set_option('display.max_columns', None)
patient_filter['ADMITTIME'] = pd.to_datetime(patient_filter['ADMITTIME']).dt.date
patient_filter['DISCHTIME'] = pd.to_datetime(patient_filter['DISCHTIME']).dt.date
patient_filter['EDREGTIME'] = pd.to_datetime(patient_filter['EDREGTIME']).dt.date
patient_filter['EDOUTTIME'] = pd.to_datetime(patient_filter['EDOUTTIME']).dt.date
patient_filter['adm_age'] = pd.DatetimeIndex(patient_filter['ADMITTIME']).year -  pd.DatetimeIndex(patient_filter['DOB']).year

patient_filter.iloc[0] #view the first row

patient_filter.loc[patient_filter['SUBJECT_ID'] == 256] #locate subject 249

#read in diagnosis for each admission
diagnoses = pd.read_csv(mimic_diagnoses)
diagnoses=diagnoses.sort_values(by=['HADM_ID', 'SEQ_NUM'])
diagnoses_1=icd.long_to_short_transformation(diagnoses, ["HADM_ID"], ["ICD9_CODE"])
# join with patient and admission records
adm_w_dx= pd.merge(patient_filter, diagnoses_1, on='HADM_ID', how='inner')
#one case only for testing
tt=adm_w_dx.loc[adm_w_dx['SUBJECT_ID']==109]

#select certain columns
tt=tt[['SUBJECT_ID','GENDER','age','HADM_ID','ADMITTIME','DISCHTIME','DIAGNOSIS','admit_times','adm_age','icd_0','icd_1','icd_2','icd_3',"icd_4","icd_5","icd_6","icd_7","icd_8","icd_9","icd_10","icd_11",
                                                  "icd_12","icd_13","icd_14","icd_15","icd_16","icd_17","icd_18","icd_19","icd_20","icd_21","icd_22","icd_23",
                                                  "icd_24","icd_25","icd_26","icd_27","icd_28","icd_29","icd_30","icd_31","icd_32","icd_33","icd_34","icd_35","icd_36","icd_37","icd_38"]]
#drop index
diagnoses_1.reset_index(drop=True)
icds=["icd_0","icd_1","icd_2","icd_3","icd_4","icd_5","icd_6","icd_7","icd_8","icd_9","icd_10","icd_11",
                                                  "icd_12","icd_13","icd_14","icd_15","icd_16","icd_17","icd_18","icd_19","icd_20","icd_21","icd_22","icd_23",
                                                  "icd_24","icd_25","icd_26","icd_27","icd_28","icd_29","icd_30","icd_31","icd_32","icd_33","icd_34","icd_35","icd_36","icd_37","icd_38"]
#df=diagnoses_1.astype({'HADM_ID': 'int32'})
diagnoses_1=diagnoses_1.fillna('')
#idx="HADM_ID"
#icds=["icd_0","icd_1","icd_2","icd_3","icd_4"]
df=diagnoses_1.sort_values(by=['HADM_ID'])
df=df.fillna('')
df.reset_index(drop=True)
#df.set_index('HADM_ID')
#df=df[['icd_0','icd_1', 'icd_2','icd_3', 'icd_4','HADM_ID']].head(n=1266)
#df=df[['icd_0','icd_1', 'icd_2','icd_3', 'icd_4','HADM_ID']].iloc[1264:1266]
#df['person_id'] = np.arange(len(df))+1
#create dx grouping indicators
comorbidity= icd.icd_to_comorbidities(df, "HADM_ID", icds, mapping="quan_elixhauser9")
#comorbidity.head(n=1)
#df.head(n=1)
comorbidity=comorbidity.reset_index(drop=True)
tt=tt.reset_index(drop=True)
# join the indicators with the patient, admission, and diagnosis codes
adm_w_dxind= pd.merge(tt, comorbidity, on=['HADM_ID'], how='inner')

adm_w_dxind['NEXT_ADM_DATE'] = adm_w_dxind['ADMITTIME'].shift(-1)
adm_w_dxind['DAYS_TO_NEXT_ADM'] = (adm_w_dxind['NEXT_ADM_DATE'] - adm_w_dxind['DISCHTIME']).dt.days
#check data types
adm_w_dxind.dtypes
adm_w_dxind.to_excel("case.xlsx")
















from importlib import reload
reload(icd)

df[df.isnull().any(axis=1)]
df.loc[df['icd_4']== '' ]#.isin('')]

icd.icd_to_comorbidities(df, "HADM_ID", ["icd_0","icd_1","icd_2","icd_3","icd_4","icd_5","icd_6","icd_7","icd_8","icd_9","icd_10","icd_11",
                                                  "icd_12","icd_13","icd_14","icd_15","icd_16","icd_17","icd_18","icd_19","icd_20","icd_21","icd_22","icd_23",
                                                  "icd_24","icd_25","icd_26","icd_27","icd_28","icd_29","icd_30","icd_31","icd_32","icd_33","icd_34","icd_35","icd_36","icd_37","icd_38"], mapping="quan_elixhauser10")

diagnoses_1.dtypes
icd.icd_to_comorbidities(diagnoses_1, "HADM_ID", ["icd_0","icd_1","icd_2","icd_3","icd_4"], mapping=custom_mapping)



df = pd.DataFrame.from_dict({'icd_0': {1: 'F1P', 2: 'F322', 3: ''},
                             'icd_1': {1: 'F11', 2: 'C77', 3: 'G10'},
                             'icd_2': {1: '', 2: 'C737', 3: ''},
                             'icd_3': {1: 'E400', 2: 'F32', 3: ''},
                             'icd_4': {1: 'E40', 2: '', 3: ''},
                             'person_id': {1: 1, 2: 2, 3: 3}})
icd.icd_to_comorbidities(df, "person_id", ["icd_0","icd_1","icd_2","icd_3","icd_4"])