import os
import json

import numpy as np
import pandas as pd
from models.base import session
from models.Models import Info
from pprint import pprint


def correct_term(term,zeelolgoson_date,tologdokhognoo_date):

    term_in = term

    if term%6 != 0:
        if term%6 > 3:
            add_part = 6-term%6
            term = add_part+term
        elif term%6 < 3:
            add_part = term%6
            term=term-add_part
        else :
            if term%4 == 3:
                add_part = 4-term%4
                term=add_part+term
            elif term%4 == 1:
                add_part = term%4
                term = term-add_part
               
    else :
        term = term

    if abs(term-term_in) > 1:
        term=term_in
    
    min_zeel_date=min(zeelolgoson_date,tologdokhognoo_date)
    max_zeel_date=max(zeelolgoson_date,tologdokhognoo_date)
    
    if (max_zeel_date-min_zeel_date < 8)|(30.0+min_zeel_date-max_zeel_date < 8):
        term = term_in
    
    return term
    

def amortization_table_total_equally(principal,rate,term):
    ''' Prints the amortization table for a loan.

    Prints the amortization table for a loan given
    the principal, the interest rate (as an APR), and
    the term (in months).'''

    payment = pmt(principal=principal, rate=rate, term=term)
    begBal = principal

    df_tmp=pd.DataFrame(columns=["Pmt", "Begbal","Payment","Principal","Interest","Endbal"])

    for num in range(1, term + 1):

        interest = round(begBal * (rate / (12 * 100.0)), 2)
        applied = round(payment - interest, 2)
        endBal = round(begBal - applied, 2)

        df_tmp.loc[num,"Pmt"]=num
        df_tmp.loc[num,"Begbal"]=begBal
        df_tmp.loc[num,"Payment"]=payment
        df_tmp.loc[num,"Principal"]=applied
        df_tmp.loc[num,"Interest"]=interest
        df_tmp.loc[num,"Endbal"]=endBal

        begBal = endBal

    return df_tmp

def pmt(principal, term,rate):
    '''Calculates the payment on a loan.

    Returns the payment amount on a loan given
    the principal, the interest rate (as an APR),
    and the term (in months).'''

    ratePerTwelve = rate / (12 * 100.0)

    result = principal * (ratePerTwelve / (1 - (1 + ratePerTwelve) ** (-term)))

    # Convert to decimal and round off to two decimal
    # places.
    # result = Decimal(result)
    result = round(result, 2)
    return result

def clean_df(df_tmp):

    df_tmp["d/d"]=df_tmp["d/d"].astype(int)
    columns=["kheviin","khugatsaakhetersen","kheviinbus","ergelzeetei","muu"]

    df_tmp[columns]=df_tmp[columns].replace('+',1)
    df_tmp[columns]=df_tmp[columns].replace(np.NaN,0)

    columns = ['zeeliinkhemzhee','oriinuldegdel']

    df_tmp.zeeliinkhemzhee=df_tmp.zeeliinkhemzhee.astype(str).str.replace(',','')
    df_tmp.oriinuldegdel=df_tmp.oriinuldegdel.astype(str).str.replace(',','')
    df_tmp.zeeliinkhemzhee=df_tmp.zeeliinkhemzhee.astype(str).str.replace('\s','')
    df_tmp.oriinuldegdel=df_tmp.oriinuldegdel.astype(str).str.replace('\s','')
    df_tmp[columns]=df_tmp[columns].astype(float)

    df_tmp.zeelolgosonognoo=pd.to_datetime(df_tmp.zeelolgosonognoo)
    df_tmp.tologdokhognoo=pd.to_datetime(df_tmp.tologdokhognoo)
    return df_tmp


def amortization_table_principal_equally(principal, rate, term, grace):
    ''' Prints the amortization table for a loan.

    Prints the amortization table for a loan given
    the principal, the interest rate (as an APR), and
    the term (in months).'''

    #payment = pmt(principal, rate, term)
    begBal = principal

    df_tmp=pd.DataFrame(columns=["Pmt", "Begbal","Payment","Principal","Interest","Endbal"])

    for num in range(1, term + 1):

        interest = round(begBal * (rate / (12 * 100.0)), 2)
        if num <= grace:
            applied = 0
        else :
            applied = principal/(term-grace)


        payment = applied+interest


        interest = round(interest, 2)
        applied = round(applied, 2)
        payment = round(applied+interest, 2)
        endBal = round(begBal - applied, 2)

        #print interest,applied,payment

        df_tmp.loc[num,"Pmt"]=num
        df_tmp.loc[num,"Begbal"]=begBal
        df_tmp.loc[num,"Payment"]=payment
        df_tmp.loc[num,"Principal"]=applied
        df_tmp.loc[num,"Interest"]=interest
        df_tmp.loc[num,"Endbal"]=endBal

        begBal = endBal

    return df_tmp

def clean_file(df_tmp_combined,downloaded_date):
    df_tmp_combined=df_tmp_combined[df_tmp_combined["d/d"].notnull()]
    df_tmp_combined=df_tmp_combined[df_tmp_combined.registriindugaar.notnull()]
    df_tmp_combined.oriinuldegdel=df_tmp_combined.oriinuldegdel.fillna(0)

    df_tmp_combined['zeelolgoson_date']=df_tmp_combined.zeelolgosonognoo.astype(str).str[-2:].astype(int)
    df_tmp_combined['tologdokhognoo_date']=df_tmp_combined.tologdokhognoo.astype(str).str[-2:].astype(int)

    df_tmp_combined.zeelolgosonognoo=pd.to_datetime(df_tmp_combined.zeelolgosonognoo)
    df_tmp_combined.tologdokhognoo=pd.to_datetime(df_tmp_combined.tologdokhognoo)

    #df_tmp_combined["downloaded_date"]=downloaded_date
    df_tmp_combined["downloaded_date"]=pd.to_datetime(df_tmp_combined["downloaded_date"])

    df_tmp_combined['term']=df_tmp_combined.tologdokhognoo-df_tmp_combined.zeelolgosonognoo
    df_tmp_combined.term=df_tmp_combined.term.astype(str).str.split(" ").str.get(0).astype(int)
    df_tmp_combined.term=((df_tmp_combined.term*1.0/30.4)).round().astype(int)  # 0.3

    df_tmp_combined=df_tmp_combined.reset_index().drop('index',1)

    for i in df_tmp_combined.index.tolist():
        term =  df_tmp_combined.loc[i,'term']
        tologdokhognoo_date =  df_tmp_combined.loc[i,'tologdokhognoo_date']
        zeelolgoson_date = df_tmp_combined.loc[i,'zeelolgoson_date']
        corrected_term = correct_term(term,zeelolgoson_date,tologdokhognoo_date)
        df_tmp_combined.loc[i,'term']=corrected_term

    df_tmp_combined=df_tmp_combined.drop(['tologdokhognoo_date','zeelolgoson_date'],1)
    

    #df_tmp_combined["term"]=df_tmp_combined.tologdokhognoo.dt.to_period('M') - df_tmp_combined.zeelolgosonognoo.dt.to_period('M')
    df_tmp_combined["months"]=df_tmp_combined["downloaded_date"]-df_tmp_combined["zeelolgosonognoo"]

    df_tmp_combined.months=df_tmp_combined.months.astype(str).str.split(' ').str.get(0)
    #df_tmp_combined.months=((df_tmp_combined.months.astype(float)/30.4)-0.3).round().astype(int) #0.3
    df_tmp_combined.months=((df_tmp_combined.months.astype(float)/30.4)).astype(int)

    tmp_index=df_tmp_combined[df_tmp_combined.term==0].index
    df_tmp_combined.loc[tmp_index,'term']=1
    df_tmp_combined=df_tmp_combined.reset_index().drop('index',1)

    df_tmp_combined=df_tmp_combined[df_tmp_combined.zeeliinkhemzhee!=0.0]
    df_tmp_combined=df_tmp_combined.reset_index().drop('index',1)

    return df_tmp_combined

def give_info(df_tmp_combined):
    df_tmp_duussan_zeeluud=df_tmp_combined[(df_tmp_combined.oriinuldegdel==0)]
    niit_kheviin_zeel=df_tmp_duussan_zeeluud.kheviin.sum()
    niit_busad_zeel=df_tmp_duussan_zeeluud.khugatsaakhetersen.sum()+\
        df_tmp_duussan_zeeluud.kheviinbus.sum()+\
        df_tmp_duussan_zeeluud.ergelzeetei.sum()+\
        df_tmp_duussan_zeeluud.muu.sum()

    df_tmp_geree_oorchlogdson_zeel = df_tmp_combined\
        [(df_tmp_combined.tologdokhognoo < df_tmp_combined.downloaded_date) & \
        (df_tmp_combined.oriinuldegdel!=0)&(df_tmp_combined.kheviin==1)]

    df_tmp_kheviinbus_odoo=df_tmp_combined[(df_tmp_combined.oriinuldegdel!=0) & \
        (df_tmp_combined.tologdokhognoo > df_tmp_combined.downloaded_date) & \
        (df_tmp_combined.kheviin==0)]

    df_tmp_odoo=df_tmp_combined[(df_tmp_combined.oriinuldegdel!=0) & \
        (df_tmp_combined.tologdokhognoo > df_tmp_combined.downloaded_date) & \
        (df_tmp_combined.kheviin==1)]

    payment=df_tmp_odoo["payment_t"].sum()

    return niit_kheviin_zeel,niit_busad_zeel,\
        df_tmp_geree_oorchlogdson_zeel.shape[0],df_tmp_kheviinbus_odoo.shape[0],\
        payment

def create_prod_mbinfos(df_tmp,column='downloaded_date'):
    df_mbinfos=df_tmp
    df_mbinfos=df_mbinfos.reset_index().drop('index',1)

    downloaded_date=df_mbinfos[column].unique()[0]
    df_mbinfos=clean_file(df_mbinfos,downloaded_date)
    df_mbinfos=df_mbinfos.reset_index().drop("index",1)
    return df_mbinfos

def choose_only_normal_loans(df_prod_mbinfos):
    df_prod_mbinfos=df_prod_mbinfos[df_prod_mbinfos.oriinuldegdel > 0]
    df_prod_mbinfos.downloaded_date=pd.to_datetime(df_prod_mbinfos.downloaded_date)
    df_prod_mbinfos.zeelolgosonognoo=pd.to_datetime(df_prod_mbinfos.zeelolgosonognoo)
    df_prod_mbinfos.tologdokhognoo=pd.to_datetime(df_prod_mbinfos.tologdokhognoo)
    df_prod_mbinfos=df_prod_mbinfos[df_prod_mbinfos.tologdokhognoo.notnull()]    
    #print(df_prod_mbinfos)
    #add by detail2
    df_prod_mbinfos=df_prod_mbinfos[df_prod_mbinfos.loantypecode=="Зээл"]
    df_prod_mbinfos=df_prod_mbinfos[df_prod_mbinfos.statuscode=="Төлөгдөж дуусаагүй"]
    df_prod_mbinfos=df_prod_mbinfos[df_prod_mbinfos.loanclasscode=="Хэвийн"]

    df_mbinfos=df_prod_mbinfos.reset_index().drop('index',1)
    #print('AAAA')
    #print(df_mbinfos)
    if len(df_prod_mbinfos)!=0:
        df_mbinfos=create_prod_mbinfos(df_mbinfos)
        df_mbinfos['d/d']=df_mbinfos['d/d'].astype(int)
        df_mbinfos['accountid']=df_mbinfos['registriindugaar'].astype(str)+'-'+df_mbinfos['d/d'].astype(str)

        df_mbinfos=df_mbinfos[df_mbinfos.kheviin==1]
        df_mbinfos=df_mbinfos[df_mbinfos.term > df_mbinfos.months]
        df_mbinfos=df_mbinfos.reset_index().drop('index',1)
    #print('BBBB')
    #print(df_mbinfos)
    # if df_mbinfos is empty, just assign empty dataframes
    if len(df_mbinfos) != 0 :
        df_mbinfos_old=df_mbinfos[((df_mbinfos.zeeliinkhemzhee==df_mbinfos.oriinuldegdel) & (df_mbinfos.months==0))==False]
        df_mbinfos_new=df_mbinfos[((df_mbinfos.zeeliinkhemzhee==df_mbinfos.oriinuldegdel) & (df_mbinfos.months==0))==True]
        df_mbinfos_old=df_mbinfos_old.reset_index().drop('index',1)
        df_mbinfos_new=df_mbinfos_new.reset_index().drop('index',1)
    else :
        df_mbinfos_old=df_mbinfos
        df_mbinfos_new=df_mbinfos

    return df_mbinfos_old,df_mbinfos_new

def create_df_from_apiserver(rnumber):
    
    q = session.query(Info)\
        .filter_by(rnumber=rnumber)\
        .filter(Info.detail.isnot(None))\
        .filter(Info.status==Info.STATUS_FINISHED)\
        .order_by(Info.created_at.desc()).all()
    rows = [u.to_tuple() for u in q]
    
    if(len(rows)==0):
        return []
    df_tmp=pd.DataFrame(rows)    
    df_tmp.columns=Info.columns()
    
    return df_tmp

def query_with_mbinfo(id):    
    row = session.query(Info).filter_by(id=id).first()
    df_tmp = pd.read_json(row.detail, orient="index")
    df_tmp = df_tmp.T
    
    
    cols=[u'd/d', u'registriindugaar', u'zeeliinkhemzhee', u'zeelolgosonognoo',
        'tologdokhognoo', u'valiutynner', u'oriinuldegdel', u'kheviin',
        'khugatsaakhetersen', u'kheviinbus', u'ergelzeetei', u'muu', u'buleg',
        'dedbuleg', u'ukhekhbg-yndugaar', u'ulsynburtgeliindugaar',
        'kartyndugaar', u'tailbar']

    df_tmp=df_tmp[cols]
    
    return df_tmp

def query_with_detail2( id ):    
    row = session.query(Info).filter_by(id=id).first()
    #print(row.detail2)
    detail2=json.loads(row.detail2)
    df_tmp=pd.DataFrame(detail2["loan"])
    return df_tmp
