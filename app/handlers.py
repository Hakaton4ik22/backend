import uuid
import pandas as pd
from datetime import datetime

from fastapi import APIRouter, Body, Depends, HTTPException
from starlette import status

from app.models import connect_db# User#, Stream, AuthToken, StreamStatus
from app.forms import UserDataUpdateForm, UserLoginForm, UserDelta


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

@router.get('/tnved_description', name='filter:tnved_category')
def take_tnved_category(database=Depends(connect_db)):

    sql = '''
    select  

        distinct td.category
    
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



#---------------------------------------------------------------------------------------

@router.post('/delta', name='user:takeDelta')
def take_delta(user_form: UserDelta = Body(..., embed=True), database=Depends(connect_db)):

    sql = '''
          select  
                o.napr, 
                o.nastranapr, 
                td.tnved_description,
                o.tnved,
                o.stoim, 
                o.netto, 
                o.kol,
                o.region,
                o."month", 
                o."year"  
            from operations o 
            join tnved_desc td on (o.tnved = td.tnved_id
                                   and td.category = '{}')
            --фильтры
            where 
            o.napr = 'ИМ'
            and o.region = 75

            ;
            '''.format(user_form.tnvedsForm)

    
    def calculate_delta(df, years=[2019, 2021], NAME=['stoim', 'netto', 'kol']):
    
        result = pd.DataFrame()
        
        for name in NAME:
            for year in range(years[0], years[1]):
                for month in range(1, 13):
                    a = name + str(year) + str(month)
                    b = name + str(year + 1) + str(month)

                    try:
                        first_second = ((df[b] - df[a]) / df[a].replace(0.0, 1)).round(2) * 100

                        SAVE = result.columns.to_list()

                        result = pd.concat([result, first_second], axis=1)
                        result.columns = [*SAVE, b + 'To' + a]
                    except:
                        print('Ошибка')
        
        return result



    
    df = pd.read_sql(sql, connect_db())
    print(df.shape, user_form.tnvedsForm)

    MOSCOW = df.pivot_table(index=['napr', 'tnved'], 
                            columns=['year', 'month'], 
                            values=['stoim', 'netto', 'kol'], 
                            aggfunc='sum'
                            )\
               .reset_index()
    COL = []
    for i in list(MOSCOW.columns):
        COL.append(str(i[0]) + str(i[1]) + str(i[2]))
    MOSCOW.columns = COL

    df = calculate_delta(MOSCOW)

    for col in df.columns:
        df[col] = df[col].apply(lambda x: str(round(x, 2)))

    return list(df.head(50).to_dict('index').values())


#-----------------------------------------------------------------------------------------

























