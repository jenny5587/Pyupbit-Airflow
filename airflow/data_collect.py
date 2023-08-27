import pyupbit
import pandas as pd
import boto3
from datetime import datetime
import numpy as np
from numpy import nan
import os
from dotenv import load_dotenv

# env 사용
load_dotenv()

def data_collect():
    ticker = 'KRW-BTC'
    interval = 'minute1'
    to = datetime.now().strftime('%Y-%m-%d %H:%M')
    count = 60
    df=pyupbit.get_ohlcv(ticker=ticker,interval=interval,to=to,count=count)
    df=df.reset_index()
    df=df.rename(columns = {'index':'collect_time'})
    os.makedirs(f'/home/ubuntu/miniproject-upbit/data/{to}/')
    df.to_csv(f'/home/ubuntu/miniproject-upbit/data/{to}/data_{to}.csv',header=True)

    aws_access_key_id = os.environ.get('aws_access_key_id')
    aws_secret_access_key = os.environ.get('aws_secret_access_key')
    # S3 클라이언트 생성
    s3 = boto3.client('s3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key= aws_secret_access_key)
    # 저장할 로컬 파일 경로와 S3 버킷, 객체 키 설정
    local_file_path = f'/home/ubuntu/miniproject-upbit/data/{to}/data_{to}.csv'
    s3_bucket = 'pyupbit-dev-s3'
    s3_object_key = f'data/data_{to}.csv'  # 원하는 S3 객체 키
    # S3에 데이터 업로드
    s3.upload_file(local_file_path, s3_bucket, s3_object_key)
    print(f'{local_file_path}을(를) {s3_bucket}/{s3_object_key}로 업로드했습니다.')
    os.remove(f'/home/ubuntu/miniproject-upbit/data/{to}/data_{to}.csv')
