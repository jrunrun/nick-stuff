import pandas as pd
import numpy as np
import datetime
import sys
import time
import locale

#locale.setlocale(locale.LC_ALL, '')

print ("data prep starting...")

# Start stopwatch
startTime = time.process_time()

# Date and Time stamp for output file
dateTimeStamp = time.strftime("%Y%m%d_%H%M")


def groupCSN(df):

    #Creat CSN_group field and set it with Pat_CSN values
    cleanDF['CSN_group'] = cleanDF['PAT_CSN']
    #Create a list of MRN's that have Admsn=Null or Admsn='1/0/00 0:00'
    uniqueMRN = np.unique(np.concatenate((cleanDF[pd.isnull(cleanDF['HOSP_ADMSN_TIME'])==True]['MRN'].unique(),cleanDF[cleanDF['HOSP_ADMSN_TIME'].astype(str) == '1/0/00 0:00']['MRN'].unique()),axis=0))
    #Cycle through each of the MRN's and set CSN_group if Admsn=Null or Admsn='1/0/00 0:00' CSN's align within the action dates of another CSN for same MRN
    for mrnValue2 in uniqueMRN:
        #process only those MRN that have >1 Pat_CSN
        if len(cleanDF[cleanDF['MRN']==mrnValue2]['PAT_CSN'].unique()) > 1: 
            #CSN list #1: uniques (Null=True) or (1/0/00; aka Null)
            uniqueCSN_null_1 = cleanDF[(pd.isnull(cleanDF['HOSP_ADMSN_TIME'])==True) & (cleanDF['MRN']==mrnValue2)]['PAT_CSN'].unique()
            uniqueCSN_null_2 = cleanDF[(cleanDF['HOSP_ADMSN_TIME'].astype(str) == '1/0/00 0:00') & (cleanDF['MRN']==mrnValue2)]['PAT_CSN'].unique()
            uniqueCSN_null = np.unique(np.concatenate((uniqueCSN_null_1,uniqueCSN_null_2),axis=0))
            #CSN list #2:  uniques (Null=False)
            uniqueCSN = cleanDF[(pd.isnull(cleanDF['HOSP_ADMSN_TIME'])==False) & (cleanDF['MRN']==mrnValue2) & (cleanDF['HOSP_ADMSN_TIME'].astype(str) != '1/0/00 0:00')]['PAT_CSN'].unique()       
            #Compare CSN in the Null list with those that have non-null values
            for csnValue_null in uniqueCSN_null:
                for csnValue in uniqueCSN:
                    #If in between range TRUE, then assign CSN_group
                    first_month = cleanDF[cleanDF['PAT_CSN']==csnValue]['action_new'].dt.month.astype(object).min()
                    last_month = cleanDF[cleanDF['PAT_CSN']==csnValue]['action_new'].dt.month.astype(object).max()
                    if df[df['PAT_CSN']==csnValue_null]['action_new'].dt.month.between(first_month, last_month, inclusive=True).any():
                        cleanDF.loc[cleanDF['PAT_CSN']==csnValue_null,'CSN_group'] = csnValue

    return cleanDF  


#input file
#rawDF=pd.read_csv('/Users/jcraycraft/Dropbox/Coding/Python/TCH/Versions/twb_v7_py_v7_1_2/data/input/BMJan7CICU Sedation 2011-2015.csv')


# Identify CSV input and script version
if len(sys.argv) < 2 :
    raise NameError('csv filename argument missing')
inputCSV = sys.argv[1];
scriptName = sys.argv[0].split(".")
outputCSV = dateTimeStamp + '_' + scriptName[0] + ".csv"

# Read CSV into dataframe

rawDF=pd.read_csv(inputCSV)

#Add RowID column
cleanDF = rawDF.copy()
cleanDF['rowID']=cleanDF.index + 1

#column names / headers mapping
_input_drug_name_ = 'DESCRIPTION'
_input_dose_ = 'DOSE'
_input_dtime_ = 'TAKEN_TIME'
_input_id_ = 'PAT_CSN'
_input_cycle_ = 'IntubationCycle'
# determine if continuous
_input_freq_ = 'FREQ_NAME'
#########################################################switch back to all lower case based off abbreviated name; check for Nick's


#beginning of edits

#trim string to first word; i.e. grab "normalized" drug name
cleanDF[_input_drug_name_] = cleanDF[_input_drug_name_].str.split().str.get(0).str.lower()

drug_fam_dict = {"fentanyl" : "Opioid", "hydromorphone": "Opioid", "morphine" : "Opioid", "hydrocodone" : "Opioid", "methadone" : "Opioid", "dexmedetomidine" : "Dex" , "midazolam": "Benzo", "ketamine": "Ketamine", "lorazepam": "Benzo", "propofol": "Propofol", "chloral": "Chloral", "atracurium": "NMB", "vecuronium": "NMB", "rocuronium": "NMB", "cisatracurium": "NMB", "clonidine": "Clonidine", "pentobarbital": "Barb", "phenobarbital": "Barb"}
cleanDF["drug_fam"] = cleanDF[_input_drug_name_].map(drug_fam_dict) 

#changed Hydro from .15 to .015  
drug_equiv_dict = {"fentanyl" : .01, "hydromorphone": .015, "morphine" : 1, "dexmedetomidine" : 1, "midazolam": 1, "lorazepam": 0.3, "hydrocodone" : 1.9, "methadone": .2, "ketamine": 1, "propofol": 1, "chloral": 1, "atracurium": 1, "cisatracurium": 1, "vecuronium": 1, "rocuronium": 1, "clonidine": 1, "pentobarbital": 1, "phenobarbital": 1}

#end of edits


cleanDF["MSO4equiv"] = cleanDF[_input_drug_name_].map(drug_equiv_dict)
cleanDF['MSO4equivDOSE'] = cleanDF['MSO4equiv'] * cleanDF[_input_dose_]

cleanDF['action_new']=pd.to_datetime(cleanDF[_input_dtime_])



#groupCSN(cleanDF)





#save non-Narcotic drugs for later
nonNarcoticDF =cleanDF[cleanDF['drug_fam'].isnull()]

sortedDF = cleanDF.groupby([_input_id_,'drug_fam']).apply(pd.DataFrame.sort_values, 'action_new').reset_index(drop=True)

#test groupby with 'FREQ_NAME'

uniqueEncounters = sortedDF[_input_id_].unique()


#try slice again here
continuousDF = sortedDF.loc[sortedDF[_input_freq_]== 'CONTINUOUS'].copy()
not_continuousDF = sortedDF.loc[sortedDF[_input_freq_]!= 'CONTINUOUS'].copy()

subAnswer = pd.DataFrame()
answerDF1 = pd.DataFrame()
answerDF2 = pd.DataFrame()

for encounterValue in uniqueEncounters:
    print("------------------------------------------------")
    #print("Processing encounterValue: " + encounterValue)
    print("------------------------------------------------")
    subAnswer = pd.DataFrame()
    subDF1 = continuousDF.loc[continuousDF[_input_id_]== encounterValue].copy()
    #subDF1 = continuousDF.loc[continuousDF['EncounterID']== '35-2'].copy()

    uniqueCycles = subDF1[_input_cycle_].unique()

    for cycleValue in uniqueCycles:
        subDF2 = subDF1.loc[subDF1[_input_cycle_]== cycleValue].copy()
        uniqueDrug = subDF2[_input_drug_name_].unique()
        #uniqueDrugFam = subDF2['drug_fam'].unique()
        
        #for drug in uniqueDrugFam:
        for drug in uniqueDrug:
            print("Processing drug: " + drug)
            subDF3 = subDF2.loc[subDF2[_input_drug_name_] == drug].copy().reset_index()

            # old line
            # subDF3['timeDelta']= (subDF3['action_new']-subDF3['action_new'].shift()).fillna(0)
            
            # new line
            subDF3['timeDelta']= (subDF3['action_new']-subDF3['action_new'].shift()).fillna(pd.Timedelta(seconds=0))

            
            subDF3['timeDeltaSec'] = 0
            subDF3['cycle'] = 1
            subDF3['cumulativeDose'] = 0
            #subDF2['MSO4equivDOSE'] = 0

            for row in range(subDF3.shape[0])[1:subDF3.shape[0]]:  ###making assumption that each subdf2 has at least two times
                subDF3.loc[row, 'timeDeltaSec'] = subDF3.loc[row, 'timeDelta'].seconds
                #subDF2.loc[row, 'cumulativeDose'] = subDF2.loc[row-1,'MSO4equiv'] * subDF2.loc[row-1,'DOSE'] * subDF2.loc[row, 'timeDeltaSec'] / 3600
                subDF3.loc[row, 'cumulativeDose'] = subDF3.loc[row-1,'MSO4equivDOSE'] * subDF3.loc[row, 'timeDeltaSec'] / 3600
                #subDF2.loc[row, 'MSO4equivDOSE'] = subDF2.loc[row,'MSO4equiv'] * subDF2.loc[row,'DOSE']
                
        #catching the cycle, but not redefining scope for time delta calc; see 2-1 and 7-1, Mid 2 (11 should be 0)

                if subDF3['timeDelta'][row]>=  datetime.timedelta(hours = 48):            
                    subDF3.loc[row, 'cycle'] = subDF3.loc[row-1,'cycle'] + 1
                else:
                    subDF3.loc[row,'cycle'] = subDF3.loc[row-1,'cycle']

            subAnswer = subAnswer.append(subDF3, ignore_index=True)
        print("Completed Drug: " + drug )        
    answerDF1 = answerDF1.append(subAnswer, ignore_index=True)
answerDF2 = answerDF2.append(answerDF1, ignore_index=True)

answerDF2['cycle_unique'] = answerDF2[_input_id_].astype('str') + "-" + answerDF2['cycle'].astype('str') 
# append on the non-continous
answerDF2 = answerDF2.append(not_continuousDF, ignore_index=True)
#append non-Narcotic drugs 
answerDF2 = answerDF2.append(nonNarcoticDF, ignore_index=True)

print("------------------------------------------------")
print("------------------------------------------------")
print ("data prep finsished...")

# Output elapsed time
print ("Elapsed:", locale.format("%.2f", time.process_time() - startTime), "seconds")
print("------------------------------------------------")
print("------------------------------------------------")



#answerDF.to_csv('/Users/jcraycraft/Dropbox/Coding/Python/TCH/Versions/twb_v7_py_v7_1_2/data/Output/master_out_v7_1_3.csv', index = False)
# Identify CSV input and script version
if len(sys.argv) < 2 :
    raise NameError('csv filename argument missing')
inputCSV = sys.argv[1];
scriptName = sys.argv[0].split(".")
outputCSV = dateTimeStamp + '_' + scriptName[0] + ".csv"

answerDF2.to_csv(outputCSV, index = False)



