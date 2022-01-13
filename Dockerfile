FROM apache/airflow:2.1.0-python3.7

RUN pip3 install yfinance==0.1.63 openpyxl pymsteams sqlalchemy tweepy==3.10.0 pymongo



