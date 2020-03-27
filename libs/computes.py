from libs import utils, utils_terminal
import pandas as pd
import numpy as np
from pprint import pprint
from models.base import session, engine
from models.Models import Ddrows, Info

#sdf
def compute1(id,rnumber):
    columns=['filename','downloaded_date','d/d', 'registriindugaar', 'zeeliinkhemzhee', 'zeelolgosonognoo',
        'tologdokhognoo', 'valiutynner', 'oriinuldegdel', 'kheviin',
        'khugatsaakhetersen', 'kheviinbus', 'ergelzeetei', 'muu', 'buleg',
        'dedbuleg', 'ukhekhbg-yndugaar', 'ulsynburtgeliindugaar',
        'kartyndugaar','tailbar']

    pd.set_option('display.max_columns',500)
    print(rnumber)
    df_date=utils.create_df_from_apiserver(rnumber)
    df_date=df_date.reset_index().drop('index',1)

    
    df_mbinfos = utils.query_with_mbinfo(id)
    df_mbinfos['downloaded_date']=df_date['created_at'].tolist()[-1]
    df_mbinfos['filename']='blabala'
    #print(df_mbinfos)    
    df_mbinfos=df_mbinfos[columns]

    df_mbinfos=df_mbinfos[columns]
    df_mbinfos=df_mbinfos.reset_index().drop('index',1)
    df_mbinfos=utils.clean_df(df_mbinfos)
    df_mbinfos=df_mbinfos.reset_index().drop('index',1)

    # added by detail2
    df_detail2=utils.query_with_detail2(id)
    df_detail2['starteddate']=pd.to_datetime(df_detail2['starteddate']).dt.strftime('%Y-%m-%d')
    df_detail2['expdate']=pd.to_datetime(df_detail2['expdate']).dt.strftime('%Y-%m-%d')
    df_detail2['starteddate']=pd.to_datetime(df_detail2['starteddate'])
    df_detail2['expdate']=pd.to_datetime(df_detail2['expdate'])
    df_detail2=df_detail2.rename(columns={"advamount":"zeeliinkhemzhee","balance":"oriinuldegdel"})
    df_detail2=df_detail2.rename(columns={"starteddate":"zeelolgosonognoo","expdate":"tologdokhognoo"})
    #print('detail2 ....')
    print(df_detail2)
    #["zeeliinkhemzhee","oriinuldegdel","zeelolgosonognoo","tologdokhognoo"]
    columns_combine=["zeeliinkhemzhee","zeelolgosonognoo","tologdokhognoo"]+\
        ["loantypecode","orgcode","statuscode","loanclasscode"]

    #print(df_mbinfos[["zeeliinkhemzhee","oriinuldegdel","zeelolgosonognoo","tologdokhognoo"]])
    #print(df_detail2[["zeeliinkhemzhee","oriinuldegdel","zeelolgosonognoo","tologdokhognoo"]])

    df_mbinfos=pd.merge(df_mbinfos,df_detail2[columns_combine],how='left')
    df_mbinfos=df_mbinfos.reset_index().drop('index',1)
    #print(df_mbinfos)
    # added end
    # only heviin odoo baigaa zeeluud
    df_mbinfos_heviin,df_mbinfos_new=utils.choose_only_normal_loans(df_mbinfos)
    #print('heviin loans calculated ...')
    #print(df_mbinfos_heviin)

    columns=['downloaded_date', 'd/d', 'registriindugaar',
        'zeeliinkhemzhee', 'zeelolgosonognoo', 'tologdokhognoo', 'valiutynner',
        'oriinuldegdel', 'kheviin', 'khugatsaakhetersen', 'kheviinbus',
        'ergelzeetei', 'muu', 'buleg', 'dedbuleg', 'ukhekhbg-yndugaar',
        'ulsynburtgeliindugaar', 'kartyndugaar',
        'tailbar', 'term', 'months', 'accountid']

    columns_pay = ['accountid','pred_tag','payment_pred',\
        'payment_pred_last','rate_mean','rate_pred','month0']
    df_payments = pd.DataFrame(columns=columns_pay)

    
    if(len(df_mbinfos_heviin)!=0):
        df_mbinfos_heviin=df_mbinfos_heviin[columns]
        
        # Payment calculation
        ac_no_list=df_mbinfos_heviin.accountid.tolist()
        print("4..")

        for j in ac_no_list:
            df_results=utils_terminal.fit_sample(df_mbinfos_heviin,j)

            df_final=utils_terminal.select_results(df_results)
            df_final=df_final[columns_pay]
            df_payments=pd.concat([df_payments,df_final])
        
        df_payments=df_payments[columns_pay]
        df_payments['registriindugaar']=df_payments.accountid.astype(str).str.split('-').str.get(0)
        df_payments['d/d']=df_payments.accountid.astype(str).str.split('-').str.get(1)

        df_payments=df_payments.drop('accountid',1)
        df_payments['d/d']=df_payments['d/d'].astype(int)

        df_payments['new_loan']=0
        print("5..")
    
    df_payments_new = pd.DataFrame(columns=columns_pay)

    if(len(df_mbinfos_new)!=0):
        df_mbinfos_new=df_mbinfos_new[columns]

        # Payment calculation
        ac_no_list=df_mbinfos_new.accountid.tolist()
        print("6..")

        for j in ac_no_list:
            df_results=utils_terminal.fit_newsample(df_mbinfos_new,j)
            print(df_results)
            df_final=utils_terminal.select_newresults(df_results)
            df_final=df_final[columns_pay]
            print('df_final',df_final)
            df_payments_new=pd.concat([df_payments_new,df_final])

        df_payments_new=df_payments_new[columns_pay]
        df_payments_new['registriindugaar']=df_payments_new.accountid.astype(str).str.split('-').str.get(0)
        df_payments_new['d/d']=df_payments_new.accountid.astype(str).str.split('-').str.get(1)

        df_payments_new=df_payments_new.drop('accountid',1)
        df_payments_new['d/d']=df_payments_new['d/d'].astype(int)
        df_payments_new['new_loan']=1

        print("7..")

    df_payments=pd.concat([df_payments,df_payments_new])
    df_payments=df_payments.reset_index().drop('index',1)
    # added df_payments = null when there is no valid 
    if len(df_payments) != 0:    
        df_mbinfos=pd.merge(df_mbinfos,df_payments,how='left')
    else :
        column_payments=['accountid', 'month0','new_loan', 'payment_pred', 
                'payment_pred_last', 'pred_tag','rate_mean', 'rate_pred']
        column_payments_obj=['accountid', 'month0','pred_tag']
        for column in column_payments:
            if column in column_payments_obj :
                df_mbinfos[column]=np.NaN
                df_mbinfos[column]=df_mbinfos[column].astype(object)
            else :
                df_mbinfos[column]=0.0
            

    print(df_mbinfos.dtypes)
    tmp_index=df_mbinfos[(df_mbinfos.kheviin!=1) & (df_mbinfos.oriinuldegdel > 0)].index
    df_mbinfos['heviinbus_uldegdeltei']=0
    df_mbinfos.loc[tmp_index,'heviinbus_uldegdeltei']=1

    tmp_index=df_mbinfos[(df_mbinfos.kheviin!=1) & (df_mbinfos.oriinuldegdel == 0)].index
    df_mbinfos['heviinbus_uldegdelgui']=0
    df_mbinfos.loc[tmp_index,'heviinbus_uldegdelgui']=1
    
    df_mbinfos.downloaded_date=pd.to_datetime(df_mbinfos.downloaded_date)

    tmp_index=df_mbinfos[(df_mbinfos.tologdokhognoo < df_mbinfos.downloaded_date) & \
        (df_mbinfos.oriinuldegdel > 0) & (df_mbinfos.kheviin==1)].index

    df_mbinfos['heviinbus_shugam']=0
    df_mbinfos.loc[tmp_index,'heviinbus_shugam']=1

    df_mbinfos=df_mbinfos.sort_values(by=['d/d'])
    df_mbinfos=df_mbinfos.reset_index().drop('index',1)
    columns=df_mbinfos.columns.tolist()

    if('pred_tag' not in columns): df_mbinfos['pred_tag']=0
    if('payment_pred' not in columns): df_mbinfos['payment_pred']=0
    if('payment_pred_last' not in columns): df_mbinfos['payment_pred_last']=0
    if('rate_mean' not in columns): df_mbinfos['rate_mean']=0    
    if('rate_pred' not in columns): df_mbinfos['rate_pred']=0
    if('month0' not in columns): df_mbinfos['month0']=0
        
    df_mbinfos = df_mbinfos.where((pd.notnull(df_mbinfos)), None)
    print(df_mbinfos)
    print(df_mbinfos.shape)

    for i,row in df_mbinfos.iterrows():
        if(isinstance(row['new_loan'], float)):
            row['new_loan']=int(row['new_loan'])  
        print(row['new_loan']) 
        Ddrows.insert(row)

           
