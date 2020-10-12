import numpy as np
import pandas as pd
import datetime
import sys
import time
import locale

##########################################################################################################################
#VPS file includes patient CSN and Intubation Cycles (each CSN has >=1 Intubation Cycle)
#MAR file includes patient CSN and Pharma data (each CSN has >=1 Pharma record)
#   Read both files as input:
#       1) For each CSN, create unique Intubation Cycles 1 thru N for N number of Intubation Cycles
#       2) Join the Pharma table to Intubation Cycles table, aligning the a TakenTime with Start/End of Intubation Cycles
##########################################################################################################################

# Date and Time stamp for output file
dateTimeStamp = time.strftime("%Y%m%d_%H%M")

#file handlers
VPS_pharma = './Test-oct12.csv'
print ("pharmacy data loaded")
VPS = './Test2-Oct12.xls'
print ("clinical data loaded")

# create dataframes
#VPS_pharma_DF=pd.read_excel(VPS_pharma)
VPS_pharma_DF=pd.read_csv(VPS_pharma)
VPS_DF=pd.read_excel(VPS)
print ('dataframes read')

#convert date/time format
VPS_pharma_DF['takenTime_new']=pd.to_datetime(VPS_pharma_DF['TAKEN_TIME'])
print ('take time read')
VPS_DF['startTime_new']=pd.to_datetime(VPS_DF['Intubation Start Date/Time'])
print ('start time read')
VPS_DF['endTime_new']=pd.to_datetime(VPS_DF['Intubation End Date/Time'])
print ('Endtime read')

#create additional columns
VPS_pharma_DF['time_tf'] = np.NaN
VPS_pharma_DF['IntubationCycle'] = np.NaN

#type cast CSN to a string
VPS_DF['CSN'] = VPS_DF['CSN'].astype('str')
VPS_pharma_DF['PAT_CSN'] = VPS_pharma_DF['PAT_CSN'].astype('str')


#type cast CSN to a string
#VPS_DF['CSN'] = VPS_DF['CSN'].astype('str')
#VPS_pharma_DF['PAT_CSN'] = VPS_pharma_DF['PAT_CSN'].astype('str')

#ID unique CSN's for each DF
CSN_VPS = VPS_DF['CSN'].unique().tolist()
print (CSN_VPS)
CSN_VPS_Pharma = VPS_pharma_DF['PAT_CSN'].unique().tolist()
print (CSN_VPS_Pharma)
print ("---------------")


# #Testing (Nov 6th, 2017):  Filtering on just a few CSN -- begin
# csn_filter = ['849331839','848674978','849409396']
# #create subframe of DF_Null, where CSN that have only 1 IntubationCycle available
# VPS_DF = VPS_DF.loc[VPS_DF['CSN'].isin(csn_filter),:].copy()
# VPS_pharma_DF = VPS_pharma_DF.loc[VPS_pharma_DF['PAT_CSN'].isin(csn_filter),:].copy()
# #ID unique CSN's for each DF
# CSN_VPS = VPS_DF['CSN'].unique().tolist()
# CSN_VPS_Pharma = VPS_pharma_DF['PAT_CSN'].unique().tolist()
# print CSN_VPS
# print CSN_VPS_Pharma
# #Testing (Nov 6th, 2017):  Filtering on just a few CSN -- end


#VPS file
#create unique IntubationCycle
for CSN_VPS_Value in CSN_VPS:
    tempDF1 = VPS_DF[VPS_DF['CSN']==CSN_VPS_Value]
    count=1
    for index, row in tempDF1.iterrows():
        VPS_DF.loc[index,'IntubationCycle'] = VPS_DF.loc[index,'CSN'] + "-" + str(count)
        count+=1

#Pharma file
#loop thru each CSN in Intubation, and group pharma taken times
for CSN_VPS_Value in CSN_VPS:

    #Filter on unique cycles for each CSN
    IntubationCycle = VPS_DF[VPS_DF['CSN']==CSN_VPS_Value]['IntubationCycle'].unique().tolist()

    #loop thru each Cycle, compare dates
    for IntubationCycle_Value in IntubationCycle:

        start = VPS_DF[VPS_DF['IntubationCycle']==IntubationCycle_Value]['startTime_new'].values[0] - np.timedelta64(1, 'h')
        end = VPS_DF[VPS_DF['IntubationCycle']==IntubationCycle_Value]['endTime_new'].values[0] + np.timedelta64(1, 'h')

        #filter VPSpharma DF based on CSN = CSN_VPS_Value
        tempDF2 = VPS_pharma_DF[VPS_pharma_DF['PAT_CSN']==CSN_VPS_Value]
        for index, row in tempDF2.iterrows():
            taken = VPS_pharma_DF.loc[index,'takenTime_new']
            if (taken > start) & (taken < end):
                VPS_pharma_DF.loc[index, 'IntubationCycle'] = IntubationCycle_Value


#create slices for complex, easy, omit cohorts where IntubationCycle is NULL
#create sub DF consisting of 39k records where IntubationCycle is NULL (at threshold above of +/- 1 hour)
DF_Null = VPS_pharma_DF.loc[VPS_pharma_DF['IntubationCycle'].isnull(),:].copy()
#indicate that there wasn't a match to IntubationCycle
DF_Null['Cycle_Match'] = "No"

#save not NULL DF for later
DF_NotNull = VPS_pharma_DF.loc[VPS_pharma_DF['IntubationCycle'].notnull(),:].copy()
#indicate that there was a match to IntubationCycle
DF_NotNull['Cycle_Match'] = "Yes"



#grab all unique CSN from DF_Null
csn_null = DF_Null['PAT_CSN'].unique().tolist()

#create list of CSN that have > 1 IntubationCycle available
csn_complex = []
for csn_val in csn_null:
    if len(VPS_DF[VPS_DF['CSN']==csn_val].index) >1:
        csn_complex.append(csn_val)
#create subframe of DF_Null_complex, where CSN that have > 1 IntubationCycle available
DF_Null_complex = DF_Null.loc[DF_Null['PAT_CSN'].isin(csn_complex),:].copy()

#create list of CSN that do NOT have an IntubationCycle available
csn_omit = []
for csn_val in csn_null:
    if len(VPS_DF[VPS_DF['CSN']==csn_val].index) == 0:
        csn_omit.append(csn_val)
#create subframe of DF_Null_omit, where CSN do NOT have an IntubationCycle available
DF_Null_omit = DF_Null.loc[DF_Null['PAT_CSN'].isin(csn_omit),:].copy()

#create list of CSN that have only 1 IntubationCycle available
csn_easy = []
for csn_val in csn_null:
    if len(VPS_DF[VPS_DF['CSN']==csn_val].index) == 1:
        csn_easy.append(csn_val)
#create subframe of DF_Null_easy, where CSN that have only 1 IntubationCycle available
DF_Null_easy = DF_Null.loc[DF_Null['PAT_CSN'].isin(csn_easy),:].copy()

print ('entering loop', csn_val, csn_complex)
print(DF_Null_complex)

# NOT SEEING ANY VALUES IN csn_complex
test = csn_val in csn_complex
print(test)

if test:
    #process complex
    #loop thru each CSN in Intubation, and group pharma taken times
    for csn_val in csn_complex:

        #create subframe for Intubation Cycles filtering on CSN
        Sub_VPS_DF = VPS_DF.loc[VPS_DF['CSN']==csn_val,:].copy()

        #grab indexes to iterate against for given CSN
        csn_idx = DF_Null_complex[DF_Null_complex['PAT_CSN']==csn_val].index.tolist()

    


        print ('entering 2nd loop')
        #loop thru each csn_idx, calc and compare deltas of takenTime vs IntStartTime
        for csn_idx_val in csn_idx:

            #grab takenTime value (as scalar value) and set entire column to value
            time_tmp = DF_Null[DF_Null.index==csn_idx_val]['takenTime_new'].values[0]
            Sub_VPS_DF['time_tmp'] = time_tmp

            #calc delta between takenTime and Intubation startTime
            Sub_VPS_DF['delta_tmp'] = Sub_VPS_DF['startTime_new'] - Sub_VPS_DF['time_tmp']
            #convert to absolute values
            Sub_VPS_DF['delta_tmp'] = Sub_VPS_DF['delta_tmp']

            #find min delta value and index
            delta_tmp = Sub_VPS_DF['delta_tmp'].min()
            #print (delta_tmp)
            index_tmp = Sub_VPS_DF['delta_tmp'].idxmin()
            #print (index_tmp)
            #grab IntubationCycle (as scalar value)
            cycle_tmp = Sub_VPS_DF[Sub_VPS_DF.index==index_tmp]['IntubationCycle'].values[0]
            #print (cycle_tmp)
            #set intubation cycle in master dataframe
            DF_Null_complex.loc[csn_idx_val,'IntubationCycle'] = cycle_tmp

        


            #set delta in master dataframe
            DF_Null_complex.loc[csn_idx_val,'GroupInt_Delta'] = delta_tmp

#type cast for GroupInt_Delta
DF_Null_complex.loc[:,'GroupInt_Delta'] = DF_Null_complex.loc[:,'GroupInt_Delta'].astype('timedelta64[D]')
DF_Null_complex.loc[:,'GroupInt_Delta'] = DF_Null_complex.loc[:,'GroupInt_Delta'].astype('int')

#process easy
#loop thru each CSN in Intubation, and group pharma taken times
for csn_val in csn_easy:

    #grab start time from VPS dataframe (as scalar value)
    startTime_tmp = VPS_DF[VPS_DF['CSN']==csn_val]['startTime_new'].values[0]

    #grab indexes for given CSN from DF_Null dataframe
    csn_idx = DF_Null[DF_Null['PAT_CSN']==csn_val].index.tolist()

    #set column to startTime in DF_Null dataframe
    DF_Null_easy.loc[csn_idx,'time_tmp'] = startTime_tmp

    #Found bug
    #DF_Null_easy['delta_tmp'] = DF_Null_easy['time_tmp'] - DF_Null_easy['time_tmp']
    #End of bug

    #Fixed on Nov 6th, 2017
    #***this should be delta between takenTime_new (Pharma data) and startTime_new (IntubationCycle data)****
    DF_Null_easy['delta_tmp'] = DF_Null_easy['takenTime_new'] - DF_Null_easy['time_tmp']
    DF_Null_easy['GroupInt_Delta'] = DF_Null_easy['delta_tmp']

    #convert to absolute values
    DF_Null_easy['GroupInt_Delta'] = DF_Null_easy['GroupInt_Delta']

    #grab IntubationCycle (as scalar value)
    cycle_tmp = VPS_DF[VPS_DF['CSN']==csn_val]['IntubationCycle'].values[0]

    #set intubation cycle in DF_Null dataframe
    DF_Null_easy.loc[csn_idx,'IntubationCycle'] = cycle_tmp

    #set delta in DF_Null dataframe
    DF_Null_easy['GroupInt_Delta'] = DF_Null_easy['delta_tmp']

#remove temporary fields from DF_Null_easy
fields = ['delta_tmp','time_tmp']
DF_Null_easy = DF_Null_easy.drop(fields, axis=1)
#type cast for GroupInt_Delta
DF_Null_easy.loc[:,'GroupInt_Delta'] = DF_Null_easy.loc[:,'GroupInt_Delta'].astype('timedelta64[D]')
DF_Null_easy.loc[:,'GroupInt_Delta'] = DF_Null_easy.loc[:,'GroupInt_Delta'].astype('int')


#define DF_Null_out
DF_Null_out = pd.DataFrame()
#append DF_Null_complex to output
DF_Null_out = DF_Null_out.append(DF_Null_complex, ignore_index=True)
#append DF_Null_easy to output
DF_Null_out = DF_Null_out.append(DF_Null_easy, ignore_index=True)
#append DF_Null_omit to output
DF_Null_out = DF_Null_out.append(DF_Null_omit, ignore_index=True)
#append DF_Null with DF_NotNull
outputDF = DF_Null_out.append(DF_NotNull, ignore_index=True)

# #convert to absolute values
# outputDF['GroupInt_Delta'] = outputDF['GroupInt_Delta'].abs()

#fill null/empty cells with -1 for Tableau filter
outputDF['GroupInt_Delta'] = outputDF['GroupInt_Delta'].fillna(-1)


#left join of dataframes
joinedDF = pd.merge(outputDF, VPS_DF, on='IntubationCycle', how='left')

#add rowID field
joinedDF['rowID']=joinedDF.index + 1

#output to CSV
joinedDF.to_csv(dateTimeStamp+'_'+'COMBO_joined_CSN.csv', index = False, encoding='utf-8')
