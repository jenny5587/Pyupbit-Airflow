from kafka import KafkaProducer
import json
import pandas as pd

server = "yourip:9092"
abalone_topic = "abalone"

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=lambda x: json.dumps(x).encode("utf-8")
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

    producer.send(abalone_topic, value=message)

producer.flush()
