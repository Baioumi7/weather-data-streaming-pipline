import json
from kafka import KafkaConsumer
import pyodbc

KAFKA_TOPIC = 'weather_data'

# SQL Server connection setup
try:
    conn = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=;'
        'DATABASE=sales;'
        'UID=;'
        'PWD='
    )
    cursor = conn.cursor()

    # Create a table if it doesn't exist
    cursor.execute('''
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='weather' AND xtype='U')
        CREATE TABLE weather (
            id INT IDENTITY(1,1) PRIMARY KEY,
            city VARCHAR(100),
            temperature FLOAT,
            description VARCHAR(100),
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    print("Connected to SQL Server and ensured the weather table exists.")
except pyodbc.Error as e:
    print(f"Failed to connect to SQL Server: {e}")
    exit(1)

# Kafka consumer setup
try:
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=['localhost:9092'],  # Ensure Kafka is accessible from this address
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    print("Connected to Kafka.")
except Exception as e:
    print(f"Failed to connect to Kafka: {e}")
    exit(1)

# Processing messages from Kafka and inserting into SQL Server
try:
    for message in consumer:
        weather_data = message.value
        city = weather_data.get('name')
        temperature = weather_data.get('main', {}).get('temp')
        description = weather_data.get('weather', [{}])[0].get('description')

        if city and temperature and description:
            cursor.execute(
                "INSERT INTO weather (city, temperature, description) VALUES (?, ?, ?)",
                (city, temperature, description)
            )
            conn.commit()
            print(f"Stored data: City={city}, Temp={temperature}, Desc={description}")
except Exception as e:
    print(f"Error processing Kafka messages: {e}")
finally:
    cursor.close()
    conn.close()
