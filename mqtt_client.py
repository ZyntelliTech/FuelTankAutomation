import json
import paho.mqtt.client as mqtt

# Configuration for MQTT
MQTT_BROKER = 'broker.hivemq.com'
MQTT_PORT = 1883
MQTT_TOPIC = 'tankAutomation/data'

# Callback for connection
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT Broker!")
        client.subscribe(MQTT_TOPIC, qos=1)  # Subscribe with QoS 1 for reliability
    else:
        print(f"Failed to connect, return code {rc}")

# Callback for receiving messages
def on_message(client, userdata, msg):
    try:
        # Parse the received JSON payload
        message_data = json.loads(msg.payload.decode())
        timestamp = message_data.get('timestamp', 'Unknown timestamp')
        data = message_data.get('data', [])

        print(f"Received message on {msg.topic}:")
        print(f"Timestamp: {timestamp}")
        for row in data:
            print(f"Data: {row}")
    except Exception as e:
        print(f"Failed to parse message: {e}")

# Main function
def main():
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(MQTT_BROKER, MQTT_PORT, 60)

    client.loop_start()  # Start the loop in a separate thread

    try:
        while True:
            pass  # Keep the program running to listen to incoming messages
    except KeyboardInterrupt:
        print("Disconnected.")
        client.loop_stop()  # Stop the loop cleanly

if __name__ == "__main__":
    main()
