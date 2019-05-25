import paho.mqtt.client as mqtt
import random, threading, json
from datetime import datetime
from time import sleep
import re
from pynput import keyboard
import socket, traceback
import time


def on_press(key):
    global break_program
    print(key)
    if key == keyboard.Key.end:
        print('end pressed')
        break_program = True
    return False


def map_msg_to_json(msg, addr):
    dic = {}
    lst = re.split("[,\'']", str(msg))
    dic['Sensor_ID'] = 'Accelerometer ' + str(addr)
    dic['Date'] = (datetime.today()).strftime("%d-%b-%Y %H:%M:%S:%f")
    dic['accX'] = lst[3].strip()
    dic['accY'] = lst[4].strip()
    dic['accZ'] = lst[5].strip()
    return json.dumps(dic)


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


def publish_to_topic(topic, message):
    mqttc.publish(topic, message)
    print("Published: " + str(message) + " " + "on MQTT Topic: " + str(topic))
    print("")


# MQTT Settings
MQTT_Broker = "iot.eclipse.org"
MQTT_Port = 1883
Keep_Alive_Interval = 30

MQTT_Topic_Acceleration = "Home/BedRoom/Topics/Acceleration"

mqttc = mqtt.Client()
mqttc.on_connect = on_connect
mqttc.on_disconnect = on_disconnect
mqttc.on_publish = on_publish
mqttc.connect(MQTT_Broker, int(MQTT_Port), int(Keep_Alive_Interval))
break_program = False
host = ''
port = 5555
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
s.bind((host, port))

dic = {}
with keyboard.Listener(on_press=on_press) as listener:
    while break_program == False:
        try:
            message, address = s.recvfrom(8192)
            acceleration_Json_Data = map_msg_to_json(message, address)
            publish_to_topic(MQTT_Topic_Acceleration, acceleration_Json_Data)
            if address not in dic:
                dic[address]=list()
                dic[address].append(acceleration_Json_Data)
            else:
                dic[address].append(acceleration_Json_Data)

            print(address,'  ',acceleration_Json_Data)
            sleep(1)  # make a pause
        except:
            traceback.print_exc()

    listener.join()
