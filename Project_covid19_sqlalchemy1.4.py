#!/usr/bin/env python
# coding: utf-8

# In[1]:


import sqlite3

import requests
import json
import sqlite3
import pandas as pd
import psycopg2
import sqlalchemy


#проверка подключения PostgreSQL
import psycopg2
import pandas as pd
#Библиотека ждя визуализации
from IPython.display import HTML
import plotly.express as px


#!введите свои реквизиты!
DB_HOST = '87.242.126.7'
DB_USER = 'student17'
DB_USER_PASSWORD = 'student17_password'
DB_NAME = 'project_covid_student17'

# conn_psql = psycopg2.connect(host=DB_HOST, user=DB_USER, password=DB_USER_PASSWORD, dbname=DB_NAME)


# выгружаем данные в пандас датафрейм (смертность, вылеченные по странам)

url = "https://coronavirus.m.pipedream.net/"

payload = {}
headers = {}

response = requests.request("GET", url, headers=headers, data=payload)
covid_json = json.loads(response.text)

df = pd.DataFrame(covid_json['rawData'])


from sqlalchemy import create_engine
engine = create_engine('postgresql+psycopg2://'+DB_USER+':'+DB_USER_PASSWORD+'@'+DB_HOST+':5432/'+DB_NAME)

# удаляем созданные вью, если существует
engine.execute('''
drop view if exists cnt_deaths_province_russia
''')

engine.execute('''
drop view if exists cnt_confirmed
''')

engine.execute('''
drop view if exists cnt_deaths
''')

engine.execute('''
drop view if exists confirmed_deaths
''')

engine.execute('''
drop view if exists covid19_cases_deaths
''')



# загружаем данные в базу данных postgresql
df.to_sql('daily_reports',
    con = engine,
    if_exists = 'replace', index=False)

# создаем вью (количество смертей по регионам в России, топ 5)
engine.execute('''
create view cnt_deaths_province_russia as
select "Province_State" as province_state, "Deaths" as deaths
from daily_reports
where "Country_Region" = 'Russia'
order by "Deaths"::int desc limit 5
''')

# создаем вью (количество зараженных по странам, топ 5)
engine.execute('''
create view cnt_confirmed as
select "Country_Region" as Country_Region, sum("Confirmed"::int) as confirmed
from daily_reports
group by "Country_Region"
order by sum("Confirmed"::int) desc limit 5
''')

# создаем вью (количество смертей по странам, топ 5)
engine.execute('''
create view cnt_deaths as
select "Country_Region" as Country_Region, sum("Deaths"::int) as deaths 
from daily_reports
group by "Country_Region"
order by sum("Deaths"::int) desc limit 5
''')

# создаем вью (количество подтвержденных тестов и количество смертей по всему миру)
engine.execute('''
create view confirmed_deaths as
select sum("Confirmed"::int) as confirmed, sum("Deaths"::int) as deaths
from daily_reports
''')

# выгружаем данные в пандас датафрейм (история смертности с начала пандемии)
url = "https://disease.sh/v3/covid-19/historical/all?lastdays=all"

payload = {}
headers = {}

response = requests.request("GET", url, headers=headers, data=payload)
covid_json = json.loads(response.text)

df1 = pd.DataFrame(covid_json)

# загружаем данные в базу postgresql 
df1.to_sql('hist_deaths',
    con = engine,
    if_exists = 'replace')

# изменяем название столбца на дату
engine.execute('''
ALTER TABLE hist_deaths RENAME COLUMN index TO date
''')

# создаем вью (количество заражений и смертей в реальном времени) и изменяем тип данных на дату
engine.execute('''
create view covid19_cases_deaths as
select date::date, cases, deaths
from hist_deaths
''')



# In[ ]:




