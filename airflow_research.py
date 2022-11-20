import pandas as pd
import vk_api 
from airflow import DAG
from airflow.operators.python_operator import PythonOperator 
from datetime import datetime
import random

default_args = {
    'owner': 'a.kaisin',
    'depends_on_past': False,
    'retries': 0,
    'start_date': datetime(2022, 10, 5),
    'schedule_interval': '0 12 * * *'
}
dag = DAG('top_10_ru_new', default_args=default_args)

def send_to_vk():
    #Считывание
    data = pd.read_csv('ads_data_121288 - ads_data_121288.csv', parse_dates=['date'])
    data.head()

    tab1 = data.groupby(['date','event'])\
        .agg({'event':'count'})\
        .rename(columns={'event':'count_event'}).reset_index()

    metrics = data.groupby(['date','event','ad_cost'])\
        .agg({'ad_cost':'sum','event':'count'})\
        .rename(columns={'ad_cost':'sum_ad_cost','event':'count_event'}).query('event == "view"').reset_index()
    metrics['sum_ad_cost'] = metrics['sum_ad_cost']/1000
    metrics['CTR'] = tab1.query("event == 'click'").reset_index().drop('index', axis = 1).count_event/ tab1.query("event == 'view'").reset_index().drop('index', axis = 1).count_event * 100

    #Расчет метрик
    message_vk = f'''
    Отчет по объявлению 121288 за 2 апреля
    Траты: {metrics.sum_ad_cost[1]} рублей ({(metrics.sum_ad_cost[1]-metrics.sum_ad_cost[0])/metrics.sum_ad_cost[0] * 100}%)
    Показы: {metrics.count_event[1]} ({(metrics.count_event[1]-metrics.count_event[0])/metrics.count_event[0]*100}%)
    Клики: {metrics.count_event[1]} ({(metrics.count_event[1]-metrics.count_event[0])/metrics.count_event[0]*100}%)
    CTR: {metrics.CTR[1]} ({(metrics.CTR[1]-metrics.CTR[0])/metrics.CTR[0]*100 }%)
    '''
    #Отправка отчета в вк
    app_token = ''
    chat_id = 1
    my_id = 42142141
    vk_session = vk_api.VkApi(token=app_token)
    vk = vk_session.get_api()

    vk.messages.send(chat_id = chat_id, random_id = random.randint(1, 2**31), message = message_vk)

    t1 = PythonOperator(task_id='metrics_task', # Название таска
                        python_callable=send_to_vk, # Название функции
                        dag=dag) # Параметры DAG