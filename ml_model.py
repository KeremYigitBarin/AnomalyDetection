# 1. Makine Öğrenmesi Modeli Geliştirme

# Gerekli kütüphanelerin yüklenmesi
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report

# Verinin okunması
file_path = r"C:\Users\kyb20\OneDrive\Masaüstü\Netflix_Data.csv"
data = pd.read_csv(file_path)

# Hedef değişkenin oluşturulması
data['Price_Change'] = data['Adj_Close'].diff().fillna(0)
threshold = data['Price_Change'].std()
data['Anomaly'] = (data['Price_Change'].abs() > threshold).astype(int)

# Özellikler ve hedefi tanımlama
X = data[['Adj_Close', 'Close', 'High', 'Low', 'Open', 'Volume']]
y = data['Anomaly']

# Veriyi eğitim ve test setlerine bölme
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42, stratify=y)

# Veriyi standartlaştırma
scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train)
X_test_scaled = scaler.transform(X_test)

# Logistic Regression modeli
lr_model = LogisticRegression()
lr_model.fit(X_train_scaled, y_train)
y_pred_lr = lr_model.predict(X_test_scaled)

# Random Forest modeli
rf_model = RandomForestClassifier(random_state=42)
rf_model.fit(X_train, y_train)
y_pred_rf = rf_model.predict(X_test)

# Sonuçların raporlanması
print("Logistic Regression Model:")
print(classification_report(y_test, y_pred_lr))

print("Random Forest Model:")
print(classification_report(y_test, y_pred_rf))

# 2. Kafka Yapılandırması
# Kafka Producer için Python kodu
from kafka import KafkaProducer
import json

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Veriyi Kafka'ya gönderme
for index, row in data.iterrows():
    message = row.to_dict()
    producer.send('anomaly_data', value=message)

# Kafka Consumer için Python kodu
from kafka import KafkaConsumer

consumer = KafkaConsumer('anomaly_results',
                         bootstrap_servers='localhost:9092',
                         value_deserializer=lambda v: json.loads(v.decode('utf-8')))

for message in consumer:
    print(message.value)

# 3. Spark Uygulaması
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Spark oturumu oluşturma
spark = SparkSession.builder.appName("KafkaSparkStreaming").getOrCreate()

# Veri şemasını tanımlama
schema = StructType([
    StructField("Date", StringType(), True),
    StructField("Adj_Close", DoubleType(), True),
    StructField("Close", DoubleType(), True),
    StructField("High", DoubleType(), True),
    StructField("Low", DoubleType(), True),
    StructField("Open", DoubleType(), True),
    StructField("Volume", DoubleType(), True),
    StructField("Price_Change", DoubleType(), True),
    StructField("Anomaly", StringType(), True)
])

# Kafka'dan veri almak
kafka_df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "anomaly_data").load()

kafka_df = kafka_df.selectExpr("CAST(value AS STRING)")
json_df = kafka_df.select(from_json(col("value"), schema).alias("data")).select("data.*")

# Gerçek zamanlı veri işleme
query = json_df.writeStream.format("console").start()
query.awaitTermination()
