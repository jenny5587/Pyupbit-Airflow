from kafka import KafkaConsumer
from json import loads
import time
import datetime
#from datetime import datetime, timezone, timedelta

consumer = KafkaConsumer("upbit",
                         bootstrap_servers=['kafka-01:9092','kafka-02:9092','kafka-03:9092'],                          auto_offset_reset="earliest", # 어디서부터 값을 읽어올지 (earlest >가장 처음 latest는 가장 최근)
                        enable_auto_commit=True, # 완료되었을 떄 문자 전송
                        value_deserializer=lambda x: loads(x.decode('utf-8')), # 역직렬화 ( >받을 떄 ) ; 메모리에서 읽어오므로 loads라는 함수를 이용한다. // 직렬화 (보낼 떄)
                        #consumer_timeout_ms=1000 # 1000초 이후에 메시지가 오지 않으면 없는 >것으로 취급.
                         )

start = time.time() # 현재 시간
print("START= ", start)
for message in consumer:
    topic = message.topic
    partition=message.partition
    offset=message.offset
    value=message.value
    timestamp=message.timestamp
    datetimeobj=datetime.datetime.fromtimestamp(timestamp/1000)
    utc_timestamp = value[0]['timestamp']
    utc_datetime = datetime.datetime.utcfromtimestamp(utc_timestamp/1000)
    kst_datetime = utc_datetime + datetime.timedelta(hours=9)
    print(kst_datetime.strftime('%Y-%m-%d %H:%M:%S'))
    #datetimeobj = datetime.datetime.fromtimestamp(kst_timestamp/1000)
    print("Topic:{}, partition:{}, offset:{}, value:{}, datetimeobj:{}".format(topic,partition,offset,value,datetimeobj))

print("Elapsed time= ",(time.time()-start)) # 걸리는 시간