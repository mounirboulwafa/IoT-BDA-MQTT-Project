import paho.mqtt.client as mqtt
import random, threading, json
from datetime import datetime


def on_connect(client, userdata, rc):
    if rc != 0:
        pass
        print("Unable to connect to MQTT Broker...")
    else:
        print("Connected with MQTT Broker: " + str(MQTT_Broker))


def on_publish(client, userdata, mid):
    pass


def on_disconnect(client, userdata, rc):
    if rc != 0:
        pass


def publish_To_Topic(topic, message):
    mqttc.publish(topic, message)
    print("Published: " + str(message) + " " + "on MQTT Topic: " + str(topic))
    print("")


# Code used as simulated Sensor to publish some random values to MQTT Broker
def getHumidityLevel(humidityValue):
    if humidityValue <= 30:
        return 'LOW'
    elif humidityValue <= 60:
        return 'MEDIUM'
    else:
        return 'HIGH'


def getTemperatureLevel(TemperatureValue):
    if TemperatureValue <= 5:
        return 'VERY COLD'
    elif TemperatureValue <= 15:
        return 'COLD'
    elif TemperatureValue <= 25:
        return 'NORMAL'
    elif TemperatureValue <= 35:
        return 'HOT'
    else:
        return 'VERY HOT'


def get_random_number():
    m = float(10)
    s_rm = 1 - (1 / m) ** 2
    return (1 - random.uniform(0, s_rm)) ** .5


def publish_sensor_values_to_mqtt():
    threading.Timer(2.0, publish_sensor_values_to_mqtt).start()
    global toggle
    if toggle == 0:
        Humidity_Value = float("{0:.2f}".format(random.uniform(10, 100) * get_random_number()))
        Humidity_Data={}
        Humidity_Data['Sensor_ID'] = "Humidity-Sensor1"
        Humidity_Data['Date'] = (datetime.today()).strftime("%d-%b-%Y %H:%M:%S:%f")
        Humidity_Data['Humidity'] = Humidity_Value
        Humidity_Data['HumidityLevel'] = getHumidityLevel(Humidity_Value)
        humidity_json_data = json.dumps(Humidity_Data)
        print("Publishing Humidity Value: " + str(Humidity_Value) + "...")
        publish_To_Topic(MQTT_Topic_Humidity, humidity_json_data)
        toggle = 1
    else:
        Temperature_Value = float("{0:.2f}".format(random.uniform(10, 100) * get_random_number()))
        Temperature_Data={}
        Temperature_Data['Sensor_ID'] = "Temperature-Sensor1"
        Temperature_Data['Date'] = (datetime.today()).strftime("%d-%b-%Y %H:%M:%S:%f")
        Temperature_Data['Temperature'] = Temperature_Value
        Temperature_Data['TemperatureLevel'] = getTemperatureLevel(Temperature_Value)
        temperature_json_data = json.dumps(Temperature_Data)
        print("Publishing Temperature Value: " + str(Temperature_Value) + "...")
        publish_To_Topic(MQTT_Topic_Temperature, temperature_json_data)
        toggle = 1
        toggle = 0


# MQTT Settings
MQTT_Broker = "iot.eclipse.org"
MQTT_Port = 1883
Keep_Alive_Interval = 30
MQTT_Topic_Humidity = "Home/BedRoom/Topics/Humidity"
MQTT_Topic_Temperature = "Home/BedRoom/Topics/Temperature"
mqttc = mqtt.Client()
mqttc.on_connect = on_connect
mqttc.on_disconnect = on_disconnect
mqttc.on_publish = on_publish
mqttc.connect(MQTT_Broker, int(MQTT_Port), int(Keep_Alive_Interval))
toggle = 0
publish_sensor_values_to_mqtt()
