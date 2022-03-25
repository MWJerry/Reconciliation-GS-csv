# Reconciliation-GS-csv
#This part of code is specifically to generate the final csv deliverable to feed the GS API
import os

from pprint import pprint
from airtable import Airtable
import pandas as pd
import numpy as np
from datetime import date
import requests
import json


today = date.today()
#import json


#installation
#pip3 install airtable-python-wrapper 
#pip3 install requests


#Step 1 Get the exceptions in Airtable
#========================================================================
base_key = 'appcfIXzQ5NuRs77L'
table_name = 'Exceptions'
api_key = 'key0KKVSkiskhOT9r'
airtable = Airtable(base_key, table_name, api_key)

#print(airtable)
records = airtable.get_all()
df_exceptions = pd.DataFrame.from_records((r['fields'] for r in records))



df_exceptions['Exception Handled?'] = df_exceptions['Exception Handled?'].fillna(0)

df_exceptions_to_handle = df_exceptions.loc[lambda x: x['Exception Handled?']==0]  #Only include the exceptions that haven't been handled

df_exceptions_to_handle = df_exceptions_to_handle.loc[:,['Exception ID','Investor Name','Investor Email','SPV Name Value', 'Intended SPV Name Value', 
                                                         'Transfer Amount','Transfer Date',
                                                         'Bank Transfer Memo', 'Exception Type','Handling Instructions', 
                                                         'Other Exception Notes','Bank Payment Type', 'Admin URL']]
df_exceptions_to_handle['Exception ID'] = df_exceptions_to_handle['Exception ID'].str.strip()
df_exceptions_to_handle = df_exceptions_to_handle.reset_index().drop('index', axis=1)




#Step 2 Get the unique bank info and filter by GS/FRB
#========================================================================
base_key_entity = 'app200b3yiBT9HU01'
table_name_banks = 'Bank Accounts'
api_key_entity = 'key0KKVSkiskhOT9r'
entity_airtable = Airtable(base_key_entity, table_name_banks, api_key_entity)

records_bank = entity_airtable.get_all()
df_entity = pd.DataFrame.from_records((r['fields'] for r in records_bank))
df_entity['Management Entity Name'] = df_entity['Management Entity Name'].fillna(0)
df_entity['Account use'] = df_entity['Account use'].fillna(0)


def map_func(entry):
    if type(entry) == int:
        return str(entry)
    
    return "".join(entry)

df_entity['Account use'] = df_entity['Account use'].apply(lambda x:map_func(x))


#get the opearting SPV bank accounts
df_entity_bank = df_entity[df_entity['Account use']!='Backup']
df_entity_bank = df_entity_bank[df_entity_bank['Institution']!= 'Synapse']
df_entity_bank = df_entity_bank[df_entity_bank['Institution']!= 'Cross River Bank']
df_entity_bank = df_entity_bank[df_entity_bank['Entity Status']!='Liquidated']


df_entity_bank = df_entity_bank.reset_index().drop('index', axis=1)
#========================================================================
#print(df_entity_bank)

#print(df)


df_exceptions_to_handle['funding_accountNumber'] = ''
df_exceptions_to_handle['payment_accountNumber'] = ''
df_exceptions_to_handle['Bank Name']=''
df_exceptions_to_handle['Intended SPV Bank Name'] = ''
df_exceptions_to_handle['funding_bic'] = ''
df_exceptions_to_handle['payment_bic'] = ''

#df_exceptions_to_handle['paymentRequestId'] = ''
#df_exceptions_to_handle['paymentEndToEndId'] = ''






#Step 3 Get the bank info for the funding SPV added into the outstanding exceptions table
#========================================================================
for i in range(len(df_exceptions_to_handle)):
    for j in range(len(df_entity_bank)):
        if str(df_exceptions_to_handle['SPV Name Value'][i]).upper() + ' LLC' == df_entity_bank['Account Name'][j]:
            df_exceptions_to_handle['funding_accountNumber'][i] = df_entity_bank['Account Number'][j]
            df_exceptions_to_handle['Bank Name'][i] = df_entity_bank['Institution'][j]
            df_exceptions_to_handle['funding_bic'][i] = df_entity_bank['SWIFT'][j]
            
            



#Step 4: Get the payment account numbers by category 
#=======================================================================


#Overpayment
#+++++++++++

df_overpayment = df_exceptions_to_handle.loc[lambda x: x['Exception Type']=='Overpayment']
df_overpayment_GS = df_overpayment.loc[lambda x: x['Bank Name'] == 'Goldman Sachs Bank']
df_overpayment_GS_confirmed = df_overpayment_GS[df_overpayment_GS['Handling Instructions'] == 'Refund Transfer Amount']

df_overpayment_GS_confirmed = df_overpayment_GS_confirmed.assign(payment_accountNumber = 240000003121) #MAS GS: Refund Account
df_overpayment_GS_confirmed['Intended SPV Name Value']='MASTERWORKS ADMINISTRATIVE SERVICES, LLC DDA'

df_overpayment_GS_confirmed = df_overpayment_GS_confirmed.assign(remittanceInfo=lambda x: x['Exception ID']+'-'+ x['Exception Type'])

df_overpayment_GS_confirmed = df_overpayment_GS_confirmed.assign(paymentRequestId=lambda x: 'OPMW' + x['SPV Name Value'].str[-3:] + 'MAS' + x['Exception ID'].str[-4:])
df_overpayment_GS_confirmed['paymentEndToEndId'] = df_overpayment_GS_confirmed['paymentRequestId']

df_overpayment_GS_confirmed['Transfer Date'] = today

df_overpayment_GS_confirmed['SPV Name Value'] = df_overpayment_GS_confirmed['SPV Name Value'].astype(str) + ' LLC'






#Direction Error
#+++++++++++

#Get the outging direction errors only for GS accounts and imform the incoming direcion errors
df_direction_error = df_exceptions_to_handle.loc[lambda x: x['Exception Type']=='Direction Error (Outgoing)']

df_direction_error_FRB = df_direction_error.loc[lambda x: x['Bank Name'] == 'First Republic Bank'] #Outgoing from FRB -> this is for the Manual Handling Process, not relevant to produce the csv



#GS data 
df_direction_error_GS = df_direction_error.loc[lambda x: x['Bank Name'] == 'Goldman Sachs Bank'] #Outgoing from GS
df_direction_error_GS['Intended SPV Bank Name']= ''
df_direction_error_GS['Exception Type'] = df_direction_error_GS['Exception Type'].replace(['Direction Error (Outgoing)'],'Direction Error')



#FRB data wragling
df_direction_error_FRB = df_direction_error.loc[lambda x: x['Bank Name'] == 'First Republic Bank'] #Outgoing from GS
df_direction_error_FRB['Intended SPV Bank Name']= ''
#df_direction_error_FRB['Exception Type'] = df_direction_error_GS['Exception Type'].replace(['Direction Error (Outgoing)'],'Direction Error')





#Assign the bank names to the outstanding exception table for direction errors
df_direction_error_GS = df_direction_error_GS.reset_index().drop('index', axis=1)


for i in range(len(df_direction_error_GS)):
    for j in range(len(df_entity_bank)):
        if str(df_direction_error_GS['Intended SPV Name Value'][i]).upper() +' LLC' == df_entity_bank['Account Name'][j]:
            df_direction_error_GS['payment_accountNumber'][i] = df_entity_bank['Account Number'][j]
            df_direction_error_GS['Intended SPV Bank Name'][i] = df_entity_bank['Institution'][j]
            df_direction_error_GS['payment_bic'][i] = df_entity_bank['SWIFT'][j]
            
            


#Same logic for FRB
df_direction_error_FRB = df_direction_error_FRB.reset_index().drop('index', axis=1)
for i in range(len(df_direction_error_FRB)):
    for j in range(len(df_entity_bank)):
        if str(df_direction_error_FRB['Intended SPV Name Value'][i]).upper() +' LLC' == df_entity_bank['Account Name'][j]:
            df_direction_error_FRB['payment_accountNumber'][i] = df_entity_bank['Account Number'][j]
            df_direction_error_FRB['Intended SPV Bank Name'][i] = df_entity_bank['Institution'][j]
            df_direction_error_FRB['payment_bic'][i] = df_entity_bank['SWIFT'][j]


df_directionerro_FRB_intended = df_direction_error_FRB.loc[lambda x: x['Intended SPV Bank Name'] == 'First Republic Bank'] #the intended SPV is FRB with sending SPV NAME as FRB


To_FRB_SPV = np.sort(df_directionerro_FRB_intended['Intended SPV Name Value'].unique())
From_FRB_SPV = np.sort(df_directionerro_FRB_intended['SPV Name Value'].unique())



df_directionerro_FRB_intended = df_direction_error_GS.loc[lambda x: x['Intended SPV Bank Name'] == 'First Republic Bank'] #the intended SPV is FRB with sending SPV NAME as GS

GS_new_FRB = df_directionerro_FRB_intended['Intended SPV Name Value'].unique()



#Confirm the intended SPV account is also GS
df_direction_error_GS = df_direction_error_GS.loc[lambda x: x['Intended SPV Bank Name'] == 'Goldman Sachs Bank']
#^All GS direction errors exceptions got for outgoing direction errors

df_direction_error_GS_confirmed = df_direction_error_GS[df_direction_error_GS['Handling Instructions']=='Transfer Funds to Intended Account'] #'confirmed' means the handling instruction is confirmed as 'Transfer funds to Intended Account'

'''
#df_DE_outgoing_GS_confrmed = df_direction_error_GS_confirmed[['Exception ID', 'SPV Name Value', 'funding_accountNumber', 'Intended SPV Name Value', 'payment_accountNumber','Transfer Amount', 'Transfer Date', 'remittanceInfo','Exception Type']]
df_DE_outgoing_GS_confrmed = df_direction_error_GS_confirmed.assign(payment_accountNumber = 240000003121)
df_DE_outgoing_GS_confrmed['Intended SPV Name Value'] = 'GS MAS Refund x3121'
'''

df_direction_error_GS_confirmed = df_direction_error_GS_confirmed.reset_index().drop('index', axis=1)
df_direction_error_GS_confirmed['Investor Name'] = df_direction_error_GS_confirmed['Investor Name'].str.strip()

df_direction_error_GS_confirmed = df_direction_error_GS_confirmed.assign(remittanceInfo = lambda x: x['Exception ID']+'-'+ x['Exception Type'])

df_direction_error_GS_confirmed = df_direction_error_GS_confirmed.assign(paymentRequestId=lambda x: 'DEMW'+ x['SPV Name Value'].str[-3:] + 
                                                                         'MW' + x['Intended SPV Name Value'].str[-3:] + x['Exception ID'].str[-4:])
df_direction_error_GS_confirmed['paymentEndToEndId'] =df_direction_error_GS_confirmed['paymentRequestId']

df_direction_error_GS_confirmed['SPV Name Value'] = df_direction_error_GS_confirmed['SPV Name Value'].astype(str)+ ' LLC'
df_direction_error_GS_confirmed['Intended SPV Name Value'] = df_direction_error_GS_confirmed['Intended SPV Name Value'].astype(str)+ ' LLC'


df_direction_error_GS_confirmed['Transfer Date'] = today


#Incomeing - Direction Error (working in process)

'''
df_DE_incoming = df_exceptions_to_handle.loc[lambda x: x['Exception Type']=='Direction Error (Incoming)']
df_DE_incoming['Investor Name'] = df_DE_incoming['Investor Name'].str.strip()
df_DE_incoming = df_DE_incoming.reset_index().drop('index', axis=1)



df_DE_incoming_GS_confirmed = pd.DataFrame()

column_names = list(df_DE_incoming.columns)
df_DE_incoming_GS_confirmed= pd.DataFrame(columns = column_names)



for i in range(len(df_DE_outgoing_GS_confrmed)):
    for j in range(len(df_DE_incoming)):
        if df_DE_outgoing_GS_confrmed['Intended SPV Bank Name'][i] == df_DE_incoming['SPV Name Value'][j] and df_DE_outgoing_GS_confrmed['Investor Name'][i] == df_DE_incoming['Investor Name'][j] 
        and df_DE_outgoing_GS_confrmed['Transfer Amount'][i] == df_DE_incoming['Transfer Amount'][j]:
            #if :
             #   if df_DE_outgoing_GS_confrmed['Transfer Amount'][i] == df_DE_incoming['Transfer Amount'][j]:
                    df_DE_incoming_GS_confirmed[i]=df_DE_incoming_GS_confirmed.append(df_DE_incoming[j], ignore_index = True)



#df_DE_incoming_GS = df_DE_incoming.loc[lambda x: x['Bank Name'] == 'Goldman Sachs Bank']
#df_DE_incoming_GS_confirmed = df_DE_incoming_GS[df_DE_incoming_GS['Handling Instructions']=='Transfer Funds to Intended SPV Account']
df_DE_incoming_GS_confirmed['Intended SPV Name Value'] = 'GS MAS Refund x3121'



df_DE_incoming_GS_confirmed = df_DE_incoming_GS_confirmed.rename(columns = {'SPV Name Value': 'Intended SPV Name Value', 'Intended SPV Name Value':'SPV Name Value',
                                                                            'funding_accountNumber':'payment_accountNumber', 'payment_accountNumber':'funding_accountNumber'})
df_DE_incoming_GS_confirmed = df_DE_incoming_GS_confirmed.assign(funding_accountNumber = 240000003121)



combine = [df_DE_outgoing_GS_confrmed, df_DE_incoming_GS_confirmed]
df_DE_GS_confirmed = pd.concat(combine)
'''

#Optional: Back-Check the outgoing and incoming



#Physical Check
#+++++++++++
'''
df_check = df_exceptions_to_handle.loc[lambda x: x['Exception Type']=='Physical Check']
#filter by GS account
df_check_GS = df_direction_error.loc[lambda x: x['Bank Name'] == 'Goldman Sachs Bank']

df_check_GS_Gallery = df_check_GS.assign(payment_accountNumber = 260000004800) #Assign the payment through GS Gallery account




df_check_GS_Gallery['Intended SPV Name Value']='MASTERWORKS ADMINISTRATIVE SERVICES, LLC DDA' #this is customized

df_overpayment_GS_confirmed = df_overpayment_GS_confirmed.assign(remittanceInfo=lambda x: x['Exception ID']+'-'+ x['Exception Type'])

df_overpayment_GS_confirmed = df_overpayment_GS_confirmed.assign(paymentRequestId=lambda x: 'OPMW' + x['SPV Name Value'].str[-3:] + 'MAS')
df_overpayment_GS_confirmed['paymentEndToEndId'] = df_overpayment_GS_confirmed['paymentRequestId']

df_overpayment_GS_confirmed['Transfer Date'] = today

df_overpayment_GS_confirmed['SPV Name Value'] = df_overpayment_GS_confirmed['SPV Name Value'].astype(str) + ' LLC'
'''




#Refund
#+++++++++++
#df_refund = df_exceptions_to_handle.loc[lambda x: x['Exception Type']=='Refund']




#Initial Cash Contribution
#+++++++++++
df_cc = df_exceptions_to_handle.loc[lambda x: x['Exception Type']=='Capital Contribution'] #cc: Capital Contribution

df_cc_GS = df_cc.loc[lambda x: x['Bank Name'] == 'Goldman Sachs Bank']
df_cc_GS['payment_accountNumber'] = df_cc_GS['funding_accountNumber']

#reset the funding accout numebr as the Gallery account
df_cc_GS['funding_accountNumber'] = 260000004800

#reset the Inteded SPV Name as the SPV Name
df_cc_GS['Intended SPV Name Value'] = df_cc_GS['SPV Name Value'].astype(str) + ' LLC'

#reset the SPV Name as the funding name - Gallery
df_cc_GS['SPV Name Value'] = 'Masterworks Gallery'



df_cc_GS = df_cc_GS.assign(remittanceInfo=lambda x: x['Exception ID']+'-'+ x['Exception Type'])
df_cc_GS = df_cc_GS.assign(paymentRequestId=lambda x: 'IC' + 'MWG'+ x['Intended SPV Name Value'].str[-3:] + x['Exception ID'].str[-4:]) #customize the payment request ID (needs to be unique)


df_cc_GS['paymentEndToEndId'] = df_cc_GS['paymentRequestId']
df_cc_GS['Transfer Date'] = today
df_cc_GS['SPV Name Value'] = df_cc_GS['SPV Name Value'].astype(str) + ' LLC'




#Step 5 Output the csvs by Category
#=======================================================================


#Overpayment
df_overpayment_csv = df_overpayment_GS_confirmed[['Exception ID', 'SPV Name Value', 'funding_accountNumber', 'Intended SPV Name Value',
                                                  'payment_accountNumber','Transfer Amount', 'Transfer Date',
                                                  'remittanceInfo','Exception Type', 'paymentRequestId', 'paymentEndToEndId', 'funding_bic', 'payment_bic']]


df_overpayment_csv = df_overpayment_csv.rename(columns = {'SPV Name Value':'funding_accountHolderName', 'Intended SPV Name Value':'payment_accountHolderName', 
                                                          'Transfer Amount':'payment_amount',
                                                          'Transfer Date':'requestedPaymentDate', 'Exception Type':'paymentPurpose'})

df_overpayment_csv = df_overpayment_csv.replace(np.nan, '', regex=True)  #remove all nan values in the entire dataframe
#df_overpayment_csv.to_csv(str(today) + ' Exceptions Overpayment Transactions Feeds.csv', index=False)




#Initial Cash Contribution


df_cc_csv = df_cc_GS[['Exception ID', 'SPV Name Value', 'funding_accountNumber', 'Intended SPV Name Value',
                                                  'payment_accountNumber','Transfer Amount', 'Transfer Date',
                                                  'remittanceInfo','Exception Type', 'paymentRequestId', 'paymentEndToEndId','funding_bic', 'payment_bic']]

df_cc_csv = df_cc_csv.rename(columns = {'SPV Name Value':'funding_accountHolderName', 'Intended SPV Name Value':'payment_accountHolderName', 
                                                          'Transfer Amount':'payment_amount',
                                                          'Transfer Date':'requestedPaymentDate', 'Exception Type':'paymentPurpose'})
df_cc_csv = df_cc_csv.replace(np.nan, '', regex=True)



#Direction Error - Outgoing
df_DE_csv = df_direction_error_GS_confirmed[['Exception ID', 'SPV Name Value', 'funding_accountNumber', 'Intended SPV Name Value', 'payment_accountNumber',
                                                'Transfer Amount', 'Transfer Date', 'remittanceInfo','Exception Type','paymentRequestId', 'paymentEndToEndId','funding_bic', 'payment_bic']]

df_DE_csv = df_DE_csv.rename(columns = {'SPV Name Value':'funding_accountHolderName', 'Intended SPV Name Value':'payment_accountHolderName', 
                                                        'Transfer Amount':'payment_amount', 'Transfer Date':'requestedPaymentDate', 'Exception Type':'paymentPurpose'})

df_DE_csv = df_DE_csv.replace(np.nan, '', regex=True)  #remove all nan values in the entire dataframe
#df_DE_csv.to_csv(str(today) + ' Direction Error Transactions Feeds.csv', index=False)

#df_DE_csv['paymentPurpose'] = df_DE_csv['paymentPurpose'].replace(['Direction Error (Outgoing)'],'Direction Error')



#Combine all the csvs into one

df_AT_upload = pd.concat([df_overpayment_csv, df_DE_csv, df_cc_csv]).reset_index().drop('index', axis=1)


df_AT_upload_89 = df_AT_upload[df_AT_upload['funding_accountHolderName'].isin(['Masterworks 089 LLC']) | df_AT_upload['payment_accountHolderName'].isin(['Masterworks 089 LLC'])]

df_AT_upload_322 = pd.concat([df_AT_upload_89, df_cc_csv]).reset_index().drop('index', axis=1)

#97, 101


#Create the csv
#df_AT_upload.to_csv(str(today) + ' Exceptions Transfers Feeds.csv', index =False)



df_AT_upload_322.to_csv(str(today) + ' Exceptions Transfers Feeds.csv', index = False)


df_ATP_upload_csv= df_AT_upload.to_csv('all pending exceptions.csv', index=False)


#Step 6 Upload the ready-to-handle dataframs back to airtable
#=======================================================================
'''
upload_json = json.dumps(df_AT_upload)
response = requests.post()
'''


