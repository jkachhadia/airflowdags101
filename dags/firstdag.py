
#Default Exports
from builtins import range
from datetime import timedelta
import requests
import os,sys
import json
from datetime import timedelta, datetime

from airflow.models import DAG
from airflow import models
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from airflow.hooks.base_hook import BaseHook
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable

#Setting and getting directory information
APP_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(APP_DIR)
print(f'ROOT DIR -> {ROOT_DIR} & APP_DIR -> {APP_DIR}')
sys.path.append(APP_DIR)
sys.path.append(ROOT_DIR)
sys.path.append('/'.join(ROOT_DIR.split('/')[:-1]))

#naming the dag 
DAG_ID='tweets_morning_ml_daily'
DAG_CONFIG_NAME='config_'+DAG_ID

#function to get connection
def conn(dag_config_name):
    return BaseHook.get_connection(dag_config_name)

#function to get configs from a connection
def get_config(param):
    return conn(DAG_CONFIG_NAME).extra_dejson.get(param)

#Generic Variables
TOKEN=models.Variable.get('twitter_token')

#Variables from connection
API_CALL= get_config('API_CALL')
TWEET_FIELDS= get_config('TWEET_FIELDS')
QUERY= get_config('TWEET_QUERY')


#params dictionary to pass it all the airflow tasks
params={
    "API_CALL":API_CALL,
    "TWEET_FIELDS":TWEET_FIELDS,
    "QUERY":QUERY,
    "TOKEN":TOKEN
}


# default args to pass in airflow
args = {
    'owner': 'Airflow',
    'start_date': datetime(2021,1,22,0,0)
}

#fucntion to wrap the get requests
def connect_to_endpoint(url, headers):
    response = requests.request("GET", url, headers=headers)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()

#function to get tweets 
def fetch_tweets(ds,**kwargs):
    data=[]
    query=kwargs['params']['QUERY']
    tweet_fields = "tweet.fields=author_id,text,created_at,geo,public_metrics,in_reply_to_user_id"
    url = "https://api.twitter.com/2/tweets/search/recent?query={}&{}&max_results=100&start_time={}T12:00:00.00Z&end_time={}T13:00:00.00Z".format(
        query, tweet_fields,ds,ds
    )
    headers = {"Authorization": "Bearer {}".format(kwargs['params']['TOKEN'])}
    json_response = connect_to_endpoint(url, headers)
    ct=1
    raw_data=json_response

    print('collect page ',ct)
    for x in raw_data['data']:data.append(x)
    while True:
        try:
            if raw_data["meta"]["next_token"]!=None:
                url = "https://api.twitter.com/2/tweets/search/recent?query={}&{}&max_results=100&start_time={}T12:00:00.00Z&end_time={}T13:00:00.00Z&next_token={}".format(
                        query, tweet_fields,ds,ds,raw_data["meta"]["next_token"])
                headers = {"Authorization": "Bearer {}".format(kwargs['params']['token'])}
                json_response = connect_to_endpoint(url, headers)
                raw_data=json.dumps(json_response, indent=4, sort_keys=True)
                raw_data=json_response
                for x in raw_data['data']:data.append(x)
                ct+=1
                print('collect page ',ct)
            else:
                break
        except:
            print("Collection Completed!")
            break
    print("Records Collected: ",len(data))
    kwargs['ti'].xcom_push(key='value', value=data)
    return 0

#function to dump tweets
def dump_tweets(ds,**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(key='value',task_ids='fetch_tweets') 
    with open(APP_DIR+'/../dump/'+'tweets_data_'+ds+'.json', 'w') as outfile:
        json.dump(data, outfile)
    print("dumped "+ds+"'s data!")
    return 0



#define dag
dag = DAG(
    dag_id=DAG_ID,
    default_args=args,
    schedule_interval='0 0 * * *',
    dagrun_timeout=timedelta(minutes=60)
)

#start task which is a dummy operator
start_operator=DummyOperator(
    task_id='start',
    dag=dag,
)

#end task which is a dummy operator
end_operator=DummyOperator(
    task_id='end',
    dag=dag,
)

#fetch tweets task
fetch_tweets_task=PythonOperator(
        task_id='fetch_tweets',
        provide_context=True,
        python_callable=fetch_tweets,
        params=params,
        retries=2,
        depends_on_past=True,
        trigger_rule=TriggerRule.ONE_SUCCESS,
        dag=dag
    )

#dump tweets task
dumptweets_task = PythonOperator(
        task_id='dump_tweets',
        provide_context=True,
        python_callable=dump_tweets,
        params=params,
        retries=2,
        depends_on_past=True,
        trigger_rule=TriggerRule.ONE_SUCCESS,
        dag=dag
    )

#flow of tasks
start_operator >> fetch_tweets_task >> dumptweets_task >> end_operator