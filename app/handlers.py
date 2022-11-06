import uuid
import pandas as pd
from datetime import datetime

from fastapi import APIRouter, Body, Depends, HTTPException
from starlette import status

from app.models import connect_db# User#, Stream, AuthToken, StreamStatus
from app.forms import UserDataUpdateForm, UserLoginForm


router = APIRouter()

#---------------------------------------------------------------------------------------

def check_pass(login: str):
    
    
    sql = "select a.\"Password\" from public.\"User\" as a where login = '{}'".format(login)
    
    return pd.read_sql(sql, connect_db()).Password[0]

#---------------------------------------------------------------------------------------


@router.options('/login', name='CHECKRULS', )
def checkcorp( ):
    print('chekup')
    return {}


#---------------------------------------------------------------------------------------


@router.post('/login', name='user:login')
def login(user_form: UserLoginForm = Body(..., embed=True), database=Depends(connect_db)):

    try:
        user = check_pass(user_form.email)
    except:
        return {'Answer': 'Not registred'}

    if not user or user_form.password != user:
        return {'error': 'Email/password doesnt match'}

    return {'auth_token': 'OK'}


#---------------------------------------------------------------------------------------

@router.post('/data', name='user:takeData')
def take_data_second(user_form: UserDataUpdateForm = Body(..., embed=True), database=Depends(connect_db)):




    sql = '''select  o.napr, 
        o.nastranapr, 
        td.tnved_description, 
        o.stoim, 
        o.netto, 
        o.kol, 
        rd.region_description, 
        rsd.region_s_description, 
        o."month", 
        o."year"  
    from operations o 
    join tnved_desc td on o.tnved = td.tnved_id 
    join region_desc rd on o.region = rd.region_id 
    join region_s_desc rsd on o.region_s = rsd.region_s_id 
    --фильтры
    --where 
    --o.napr = 'ЭК'
    -- сортировка
    --order by o.operation_id, o.kol [столбец сортировки] desc [по убыванию] 
    limit 100;'''

    


    return list(pd.read_sql(sql, connect_db()).to_dict('index').values())


#---------------------------------------------------------------------------------------

@router.get('/data', name='user:Firstly_takeData')
def take_data_first(database=Depends(connect_db)):

    sql = '''select  o.napr, 
        o.nastranapr, 
        td.tnved_description, 
        o.stoim, 
        o.netto, 
        o.kol, 
        rd.region_description, 
        rsd.region_s_description, 
        o."month", 
        o."year"  
    from operations o 
    join tnved_desc td on o.tnved = td.tnved_id 
    join region_desc rd on o.region = rd.region_id 
    join region_s_desc rsd on o.region_s = rsd.region_s_id 
    --фильтры
    --where 
    --o.napr = 'ЭК'
    -- сортировка
    --order by o.operation_id, o.kol [столбец сортировки] desc [по убыванию] 
    limit 100;'''

    return list(pd.read_sql(sql, connect_db()).to_dict('index').values())


#---------------------------------------------------------------------------------------

@router.get('/tnved_description', name='filter:tnved_description')
def take_tnved_desc(database=Depends(connect_db)):

    sql = '''
    select  

        distinct td.tnved_description
    
    from operations o 
        join tnved_desc td 
        on o.tnved = td.tnved_id 

    ;'''

    return [list(i.values())[0] for i in pd.read_sql(sql, connect_db()).to_dict('index').values()]


#---------------------------------------------------------------------------------------

@router.get('/country', name='filter:country')
def take_country(database=Depends(connect_db)):

    sql = '''
    
    select  

    distinct o.nastranapr
    
    from operations o

    ;'''

    return pd.read_sql(sql, connect_db()).to_string(header=False, index=False).replace('\n', '').strip().split()














