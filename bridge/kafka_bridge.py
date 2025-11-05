from kafka import KafkaProducer
import paho.mqtt.client as mqtt
import json

# MQTT Settings
MQTT_BROKER = "10.101.186.200" # MQTT broker VM IP
MQTT_TOPIC = "weather/data"

# KAFKA Settings
KAFKA_BROKER = "10.101.236.69:9092" # Kafka VM IP and port
KAFKA_TOPIC = "weather-data"

# Kafka Producer
producer = KafkaProducer(
	bootstrap_servers=[KAFKA_BROKER],
	value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# MQTT Callbacks
def on_connect(client, userdata, flags, rc):
	print("Connected to MQTT broker with result code", rc)
	client.subscribe(MQTT_TOPIC)

def on_message(client, userdata, msg):
	try:
		payload = msg.payload.decode('utf-8')
		print(f"Received MQTT message: {payload}")
		data = json.loads(payload)
		producer.send(KAFKA_TOPIC, value=data)
		print("Sent to Kafka topic:", KAFKA_TOPIC)
	except Exception as e:
		print("Error:", e)

# Connect to MQTT
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.connect(MQTT_BROKER, 1883, 60)

client.loop_forever()
