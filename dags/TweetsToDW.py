from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from datetime import datetime
from time import time
import sqlalchemy
import glob
import os
import json

default_args = {
    'owner': 'Fabricio',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 12, 13),
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
    'catchup': False, 
    'do_xcom_push':False
}


def get_file_name():
    list_of_files = glob.glob('/opt/airflow/*.txt') # * means all if need specific format then *.csv
    latest_file = max(list_of_files, key=os.path.getctime)
    return latest_file

def load_tweets():

    with open(get_file_name(), 'r') as file:
        tweets = file.readlines()       

    parsed_tweets = [json.loads( json.loads(i) ) for i in tweets]
    return parsed_tweets

def tweet_para_df(tweet):
   try:
    df_tratado = pd.DataFrame(tweet).reset_index(drop=True).iloc[:1]
    df_tratado.drop(columns=['quote_count','reply_count', 'retweet_count', 'favorite_count', 'favorited',
                                'user', 'entities', 'truncated', 'in_reply_to_user_id', 'in_reply_to_status_id',
                                 'in_reply_to_status_id_str', 'in_reply_to_user_id_str', 'in_reply_to_screen_name','contributors', 'retweeted_status'], inplace=True)
    df_tratado['user_id'] = tweet['user']['id']
    df_tratado['user_id_str'] = tweet['user']['id_str'] 
    df_tratado['user_screen_name'] = tweet['user']['screen_name']
    df_tratado['user_location'] = tweet['user']['location']
    df_tratado['user_description'] = tweet['user']['description'] 
    df_tratado['user_protected'] = tweet['user']['protected'] 
    df_tratado['user_verified'] = tweet['user']['verified'] 
    df_tratado['user_followers_count'] = tweet['user']['followers_count'] 
    df_tratado['user_friends_count'] = tweet['user']['friends_count'] 
    df_tratado['user_created_at'] = tweet['user']['created_at'] 

    user_mentions = []
    for i in range(len(tweet['entities']['user_mentions'])):
      dicionariobase = tweet['entities']['user_mentions'][i].copy()
      dicionariobase.pop('indices', None)
      df = pd.DataFrame(dicionariobase, index=[0])
      df = df.rename(columns={
          'screen_name' : 'entities_screen_name',
          'name' : 'entities_name',
          'id' : 'entities_id',
          'id_str' : 'entities_id_str'
      })
      user_mentions.append(df)

    dfs = []
    for i in user_mentions:
      dfs.append(
          pd.concat([df_tratado.copy(), i], axis=1)
      )
    df_final = pd.concat(dfs, ignore_index=True)
   except:
    return None
   return df_final


def load_data_frame():

    parseados = [tweet_para_df(tweet) for tweet in load_tweets()]
    parseados = [i for i in parseados if i is not None]
    tratado = pd.concat(parseados, ignore_index=True)
    return tratado


def load_tweets_into_db():
    load_data_frame().to_csv("tweets.csv")
    engine = sqlalchemy.create_engine(
        "postgresql://airflow:airflow@172.20.0.2:5432/dataengwarehouse" ## IP Generated by docker network
    )
    load_data_frame().to_sql("tweets", con=engine, index=False, if_exists='append')
    

with DAG(dag_id='Load_Tweets_DW', schedule_interval="*/15 * * * *", default_args=default_args, catchup=False) as dag:

    task_collect_tweets = PythonOperator(
        task_id= "CollectTweets",
        python_callable=get_file_name,
        dag=dag
    )

    task_load_tweets = PythonOperator(
        task_id= "LoadTweets",
        python_callable=load_tweets,
        dag=dag
    )

    task_load_data_frame = PythonOperator(
        task_id= "LoadDf",
        python_callable=load_data_frame,
        dag=dag
    )


    task_load_tweets_into_db = PythonOperator(
        task_id= "LoadTweetsToDb",
        python_callable=load_tweets_into_db,
        dag=dag
    )


task_collect_tweets >> task_load_tweets >> task_load_data_frame >> task_load_tweets_into_db