from libs import utils, utils_terminal
import pandas as pd
from pprint import pprint

from models.base import session, engine
from models.Models import Ddrows, Info

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
    columns_combine=["zeeliinkhemzhee","oriinuldegdel","zeelolgosonognoo","tologdokhognoo"]+\
        ["loantypecode","orgcode","statuscode","loanclasscode"]

    df_mbinfos=pd.merge(df_mbinfos,df_detail2[columns_combine],how='left')
    df_mbinfos=df_mbinfos.reset_index().drop('index',1)
    # added end

    # only heviin odoo baigaa zeeluud
    df_mbinfos_heviin,df_mbinfos_new=utils.choose_only_normal_loans(df_mbinfos)
    
    columns=['downloaded_date', 'd/d', 'registriindugaar',
        'zeeliinkhemzhee', 'zeelolgosonognoo', 'tologdokhognoo', 'valiutynner',
        'oriinuldegdel', 'kheviin', 'khugatsaakhetersen', 'kheviinbus',
        'ergelzeetei', 'muu', 'buleg', 'dedbuleg', 'ukhekhbg-yndugaar',
        'ulsynburtgeliindugaar', 'kartyndugaar',
        'tailbar', 'term', 'months', 'accountid']
    
    if(len(df_mbinfos_heviin)!=0):
        df_mbinfos_heviin=df_mbinfos_heviin[columns]
        
        # Payment calculation
        ac_no_list=df_mbinfos_heviin.accountid.tolist()
        print("4..")
        columns = ['accountid','pred_tag','payment_pred',\
            'payment_pred_last','rate_mean','rate_pred','month0']
        df_payments = pd.DataFrame(columns=columns)
        for j in ac_no_list:
            df_results=utils_terminal.fit_sample(df_mbinfos_heviin,j)
            
            df_final=utils_terminal.select_results(df_results)
            df_final=df_final[columns]
            df_payments=pd.concat([df_payments,df_final])
        
        df_payments=df_payments[columns]
        df_payments['registriindugaar']=df_payments.accountid.astype(str).str.split('-').str.get(0)
        df_payments['d/d']=df_payments.accountid.astype(str).str.split('-').str.get(1)

        df_payments=df_payments.drop('accountid',1)
        df_payments['d/d']=df_payments['d/d'].astype(int)
        print("5..")
        df_mbinfos=pd.merge(df_mbinfos,df_payments,how='left')
    
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
    for i,row in df_mbinfos.iterrows():
             
        Ddrows.insert(row)
