import pandas as pd
import numpy as np
from libs import utils
from iminuit import Minuit
import time

global principal_corr,term_corr,oriinuldegdel_corr

oriinuldegdel_corr=[]

global rate_prin

rate_prin =20.0

def func_total_LL_two(rate,month0):

    df_tmp_total=utils.amortization_table_total_equally\
        (principal=principal_corr, rate=rate, term=round(term_corr)) #term=int(a[2])

    months_corr=[month0]

    LL= 0.0
    for i in range(len(oriinuldegdel_corr)):
        LL=LL+pow(1.0-df_tmp_total.loc[round(months_corr[i]),"Endbal"]/oriinuldegdel_corr[i],2)*100*100
    
    return LL


def func_principal_LL_one(month0):
    df_tmp_principal=utils.amortization_table_principal_equally\
        (principal=principal_corr, rate=rate_prin, term=round(term_corr),grace=0) #round(grace)

    months_corr=[month0]

    LL= 0.0
    for i in range(len(oriinuldegdel_corr)):
        LL=LL+pow(1.0-df_tmp_principal.loc[round(months_corr[i]),"Endbal"]/oriinuldegdel_corr[i],2)*100*100
    
    return LL


def create_prod_df(df_tmp,accountid):
    df_tmp=df_tmp[df_tmp.accountid==accountid]
    df_tmp=df_tmp.reset_index().drop('index',1)
    
    principal=df_tmp['zeeliinkhemzhee'].tolist()[0]
    term=df_tmp['term'].tolist()[0]

    months_list=df_tmp['months'].tolist()
    oriinuldegdel_list=df_tmp['oriinuldegdel'].tolist()

    for i in range(len(months_list)):
        if (principal!=oriinuldegdel_list[i])&(months_list[i]==0):
            months_list[i]=1

    return accountid,principal,term,months_list,oriinuldegdel_list


def fit_sample(df_mbinfos,j):    
    df_results = pd.DataFrame(columns=\
        ['accountid','principal','oriinuldegdel','term','month0','month1',
        'rate_mean','rate_error','month0_mean','month1_mean','fval','payment_pred',
        'grace4','month3','month4','fval3','fval4',
        'payment_prin_mean3'])
    i=0
    accountid,principal,term,months,oriinuldegdel=create_prod_df(df_mbinfos,j)
    print(accountid,principal,term,months,oriinuldegdel)  
    global term_corr
    term_corr=term
    global principal_corr
    principal_corr=principal
    
    months_corr=[month+0 for month in months]
    months_corr=months_corr[-1:]
     
    print(months_corr,principal_corr)

    global oriinuldegdel_corr 
    oriinuldegdel_corr=oriinuldegdel[-1:]

    months_min=months[0]-1
    months_max=months[0]+1

    if months_min < 0:
        months_min=0

    start = time.time()
    m1 = Minuit(func_total_LL_two, rate=18.0, error_rate=0.1, limit_rate=(12, 25),
        month0=months[0], error_month0=0.1, limit_month0=(months_min, months_max),
        #month1=months[0], error_month1=1.0, limit_month1=(months[1]-1, months[1]+1),
        #term=term, error_term=1.0, limit_term=(term-2,term+2),
    errordef=1)
    m1.migrad()
    end = time.time()
    print("m1 {} {}".format(j,end-start))

    start = time.time()
    m2 = Minuit(func_total_LL_two, rate=8.0, error_rate=0.1, limit_rate=(3, 10),
        month0=months[0], error_month0=1.0, limit_month0=(months_min, months_max),
        #month1=months[0], error_month1=1.0, limit_month1=(months[1]-1, months[1]+1),
        #term=term, error_term=1.0, limit_term=(term-2, term+2),
        errordef=1)
    m2.migrad()
    end = time.time()
    print("m2 {} {}".format(j,end-start))
    
    start = time.time()
    m3 = Minuit(func_principal_LL_one,
        #grace=0.0, error_grace=0.1, limit_grace=(0.0, 12.0),
        month0=months[0], error_month0=0.1, limit_month0=(months_min, months_max),
        #month1=months[0], error_month1=1.0, limit_month1=(months[1]-1, months[1]+1),
        #term=term, error_term=1.0, limit_term=(term-2, term+2),
        errordef=1)
    m3.migrad()
    end = time.time()
    print("m3 {} {}".format(j,end-start))    
    
    fval1=float(m1.get_fmin()['fval'])
    fval2=float(m2.get_fmin()['fval'])

    log_fval1=np.log(fval1+1)
    log_fval2=np.log(fval2+1)
    log_fval_ratio=log_fval1/log_fval2

    month0_mean=months[0]
    month1_mean=0 #months[1]


    fval3=float(m3.get_fmin()['fval'])
    month3=round(float(m3.fitarg['month0']))

    payment_prin_mean3=float(principal)*0.20/12.0+float(principal)/term_corr
    payment_prin_max3=float(principal)*0.25/12.0+float(principal)/term_corr

    if log_fval_ratio > 50.0 :
            error_rate = float(m2.fitarg['error_rate'])
            rate_mean = float(m2.fitarg['rate'])

            month0_mean = round(float(m2.fitarg['month0']))
            #month1_mean = round(float(m2.fitarg['month0'])+months[1]-months[0])
            fval = float(m2.get_fmin()['fval'])
    else :
            error_rate = float(m1.fitarg['error_rate'])
            rate_mean = float(m1.fitarg['rate'])
            month0_mean = round(float(m1.fitarg['month0']))
            #month1_mean = round(float(m1.fitarg['month0'])+months[1]-months[0])
            fval = float(m1.get_fmin()['fval'])

    df_results.loc[i,'accountid']=accountid
    df_results.loc[i,'principal']=float(principal)
    df_results.loc[i,'oriinuldegdel']=oriinuldegdel_corr

    df_results.loc[i,'term']=float(term_corr)
    df_results.loc[i,'month0']=months[0]
    df_results.loc[i,'month1']=0 #months[1]

    df_results.loc[i,'rate_error']=error_rate
    df_results.loc[i,'rate_mean']=rate_mean

    df_results.loc[i,'month0_mean']=month0_mean
    df_results.loc[i,'month1_mean']=month1_mean
    df_results.loc[i,'fval']=fval

    payment_pred=df_results.loc[i:i].apply\
            (lambda row: utils.pmt(row['principal'],row['term'],row['rate_mean']),\
            axis=1).tolist()[0]

    df_results.loc[i:i,'payment_pred']=payment_pred

    df_results.loc[i,'month3']=round(float(m3.fitarg['month0']))
    df_results.loc[i,'fval3']=fval3

    df_results.loc[i,'payment_prin_mean3']=payment_prin_mean3
    df_results.loc[i,'payment_prin_max3']=payment_prin_max3

    i=i+1

    columns_float=['principal','term','rate_mean','rate_error','fval','fval3','payment_pred']
    df_results[columns_float]=df_results[columns_float].astype(float)
    
    return df_results

def fit_newsample(df_mbinfos,j):
    df_results = pd.DataFrame(columns=\
        ['accountid','principal','oriinuldegdel','term','month0','month1',
        'rate_mean','rate_error','month0_mean','month1_mean','fval','payment_pred',
        'grace4','month3','month4','fval3','fval4',
        'payment_prin_mean3'])
    i=0
    accountid,principal,term,months,oriinuldegdel=create_prod_df(df_mbinfos,j)
    print(accountid,principal,term,months,oriinuldegdel)
    global term_corr
    term_corr=term
    global principal_corr
    principal_corr=principal

    months_corr=[month+0 for month in months]
    months_corr=months_corr[-1:]

    print(months_corr,principal_corr)

    global oriinuldegdel_corr
    oriinuldegdel_corr=oriinuldegdel[-1:]

    months_min=months[0]-1
    months_max=months[0]+1

    if months_min < 0:
        months_min=0

    payment_prin_mean3=float(principal)*0.20/12.0+float(principal)/term_corr
    payment_prin_max3=float(principal)*0.25/12.0+float(principal)/term_corr

    df_results.loc[i,'accountid']=accountid
    df_results.loc[i,'principal']=float(principal)
    df_results.loc[i,'oriinuldegdel']=oriinuldegdel_corr

    df_results.loc[i,'term']=float(term_corr)
    df_results.loc[i,'month0']=0
    df_results.loc[i,'month1']=0 #months[1]

    df_results.loc[i,'rate_error']=0.0
    df_results.loc[i,'rate_mean']=rate_prin

    df_results.loc[i,'month0_mean']=0.0
    df_results.loc[i,'month1_mean']=0.0
    df_results.loc[i,'fval']=0.0

    payment_pred=df_results.loc[i:i].apply\
            (lambda row: utils.pmt(row['principal'],row['term'],row['rate_mean']),\
            axis=1).tolist()[0]

    df_results.loc[i:i,'payment_pred']=payment_pred

    df_results.loc[i,'month3']=months[0]
    df_results.loc[i,'fval3']=0.0
    df_results.loc[i,'payment_prin_mean3']=payment_prin_mean3
    df_results.loc[i,'payment_prin_max3']=payment_prin_max3

    i=i+1

    columns_float=['principal','term','rate_mean','rate_error','fval','fval3','payment_pred']
    df_results[columns_float]=df_results[columns_float].astype(float)

    return df_results




def select_results(df_noat):

    import dill as pickle
    
    with open('models/classifier_glm_v3.pk','rb') as f:        
        loaded_clf = pickle.load(f)
    
    with open('models/reggesser_gbm_total_v3.pk','rb') as f:
        loaded_reg = pickle.load(f)

    df_noat=df_noat.reset_index().drop('index',1)

    df_noat['log_fval']=np.log(df_noat.fval+1)
    df_noat['log_fval3']=np.log(df_noat.fval3+1)

    df_noat['log_fval_ratio']=df_noat['log_fval3']/(df_noat['log_fval']+1)

    df_noat.rate_error=df_noat.rate_error.fillna(0.0)
    df_noat.oriinuldegdel=df_noat.oriinuldegdel.astype(str).\
        str.replace('[','').str.replace(']','').astype(float)

    columns =['principal', 'oriinuldegdel', 'term', 'month0', 'rate_mean',
           'rate_error', 'month3', 'log_fval', 'log_fval3', 'log_fval_ratio']

    X=df_noat[columns].as_matrix()
    y_pred=loaded_clf.predict(X)
    df_noat['pred_tag']=y_pred

    df_noat_tot=df_noat[df_noat.pred_tag==1]
    df_noat_prin=df_noat[df_noat.pred_tag==0]
    df_noat_tot=df_noat_tot.reset_index().drop('index',1)
    df_noat_prin=df_noat_prin.reset_index().drop('index',1)
    
    if len(df_noat_tot)!=0:
        X=df_noat_tot[columns].as_matrix()

        y_pred=loaded_reg.predict(X)    
        df_noat_tot['rate_pred']=y_pred

        df_noat_tot["payment_pred"]=df_noat_tot.apply\
                (lambda row: utils.pmt(row['principal'],row['term'],row['rate_pred']), axis=1)

        df_noat_tot['payment_pred_first']=df_noat_tot.payment_pred
        df_noat_tot['payment_pred_last']=df_noat_tot.payment_pred

        #df_noat_tot['pred_tag']="Total Equally"

    if len(df_noat_prin)!=0:
        df_noat_prin=df_noat_prin.reset_index().drop('index',1)

        df_noat_prin['payment_pred_first']=np.NaN
        df_noat_prin['payment_pred_last']=np.NaN

        for i in range(len(df_noat_prin)):
            principal = df_noat_prin.loc[i,'principal']
            term = int(df_noat_prin.loc[i,'term'])
            month = int(round(df_noat_prin.loc[i,'month3']))
            

            df_tmp2=utils.amortization_table_principal_equally\
                 (principal,rate_prin,term,0)

            payment_pred_first=df_tmp2.loc[1,'Payment']
            payment_pred_last=df_tmp2.loc[month,'Payment']

            df_noat_prin.loc[i,'payment_pred_first']=payment_pred_first
            df_noat_prin.loc[i,'payment_pred_last']=payment_pred_last

        df_noat_prin['payment_pred']=df_noat_prin['payment_pred_first']
        df_noat_prin['rate_pred']=rate_prin

        #df_noat_prin['pred_tag']="Principal Equally"

    df_noat=pd.concat([df_noat_prin,df_noat_tot])

    return df_noat


def select_newresults(df_noat_prin):

    columns =['principal', 'oriinuldegdel', 'term', 'month0', 'rate_mean',
           'rate_error', 'month3', 'log_fval', 'log_fval3', 'log_fval_ratio']

    df_noat_prin['pred_tag']=0

    if len(df_noat_prin)!=0:
        df_noat_prin=df_noat_prin.reset_index().drop('index',1)

        df_noat_prin['payment_pred_first']=np.NaN
        df_noat_prin['payment_pred_last']=np.NaN

        for i in range(len(df_noat_prin)):
            principal = df_noat_prin.loc[i,'principal']
            term = int(df_noat_prin.loc[i,'term'])
            month = int(round(df_noat_prin.loc[i,'month3']))

            df_tmp2=utils.amortization_table_principal_equally\
                 (principal,rate_prin,term,0)

            payment_pred_first=df_tmp2.loc[1,'Payment']
            payment_pred_last=payment_pred_first

            df_noat_prin.loc[i,'payment_pred_first']=payment_pred_first
            df_noat_prin.loc[i,'payment_pred_last']=payment_pred_last

        df_noat_prin['payment_pred']=df_noat_prin['payment_pred_first']
        df_noat_prin['rate_pred']=rate_prin

    return df_noat_prin  
