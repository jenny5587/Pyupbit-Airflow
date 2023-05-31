from kafka import KafkaProducer
import json
import pandas as pd

server = "yourip:9092"
abalone_topic = "abalone"

producer = KafkaProducer(ask=0, #메시지 받은 사람이 메시지를 잘 받았는지 체크하는 옵션 (0은 그냥 보내기만 한다. 확인x)
    compression_type='gzip',#메시지 전달할 때 압축
    bootstrap_servers=[server] #전달하고자하는 카프카 브로커의 위치,
    value_serializer=lambda x: json.dumps(x).encode("utf-8") #직렬화 : 데이터 전송을 위해 byte단위로 바꿔주는 작업 : 
                                                               #dumps 함수이용. dump : json 값을 메모리에 올려준다. encode를 통해서 올린다.
                                                               #x가 있으면, x를 dumps로 바꾸고 encode 한다.
)

df = pd.read_csv("/home/ubuntu/abalone.csv")

for _, row in df.iterrows():
    message = {
        "Sex": row["Sex"],
        "Length": row["Length"],
        "Diameter": row["Diameter"],
        "Height": row["Height"],
        "Whole weight": row["Whole weight"],
        "Shucked weight": row["Shucked weight"],
        "Viscera weight": row["Viscera weight"],
        "Shell weight": row["Shell weight"],
        "Rings": row["Rings"]
    }

    producer.send(abalone_topic, value=message) #토픽 보낸다

producer.flush() # 비우는 작업.
