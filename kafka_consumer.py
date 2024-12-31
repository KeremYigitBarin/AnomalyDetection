from kafka import KafkaConsumer
import json

# Kafka Consumer tanımı
consumer = KafkaConsumer('anomaly_results',
                         bootstrap_servers='localhost:9092',
                         value_deserializer=lambda v: json.loads(v.decode('utf-8')))

# Gelen mesajların okunması
for message in consumer:
    print(f"Alınan mesaj: {message.value}")
