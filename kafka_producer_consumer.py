from kafka import KafkaProducer
import json
import pandas as pd

# Kafka Producer tanımı
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Veri setini okuyun
file_path = r"C:\Users\kyb20\OneDrive\Masaüstü\Netflix_Data.csv"
data = pd.read_csv(file_path)

# Anomali tespiti ekleyin
data['Price_Change'] = data['Adj_Close'].diff().fillna(0)
threshold = data['Price_Change'].std()
data['Anomaly'] = (data['Price_Change'].abs() > threshold).astype(int)

# Veriyi Kafka'ya gönderin
for index, row in data.iterrows():
    producer.send('anomaly_data', value=row.to_dict())
    print(f"Gönderilen veri: {row.to_dict()}")
