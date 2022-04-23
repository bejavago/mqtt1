from paho.mqtt import client as mqtt_client
import random
import json
import os
import time
import csv
from time import sleep
#from django_intro.plantillal33.models import *

broker = '172.31.33.83'
#broker = '192.168.1.11'
port1 = 1883
topic = "api/request"
topic_sub = "api/notification/37/#"
# generate client ID with pub prefix randomly
client_id = f'username{random.randint(0, 100)}'
#client_id = 'username0001'
username = 'MQTT_username'
password = '12345678'
deviceId = "s3s9TFhT9WbDsA0CxlWeAKuZykjcmO6PoxK6"


def connect_mqtt():

    def on_connect(client, userdata, flags, rc):
        if rc==0:
            print("Successfully connected to MQTT broker")
        else:
            print("Failed to connect, return code %d", rc)

    print(client_id)
    client = mqtt_client.Client(client_id)
    clean_session=True
    client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port1)
    return client

def publish(client, status):
    msg = "{\"action\":\"command/insert\",\"deviceId\":\""+deviceId+"\",\"command\":{\"command\":\"LED_control\",\"parameters\":{\"led\":\""+status+"\"}}}"
    result = client.publish(msg,topic)
    msg_status = result[0]
    if msg_status ==0:
        print(f"message : {msg} sent to topic {topic}")
    else:
        print(f"Failed to send message to topic {topic}")

def subscribe(client: mqtt_client):
    def on_message(client, userdata, msg):
        #print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")

        y = json.loads(msg.payload.decode('UTF8'))
        temp = y["notification"]["parameters"]["temp"]
        hum = y["notification"]["parameters"]["humi"]
        deviceiidd = y["deviceId"]
        print("Device: ",deviceiidd,"temperature: ",temp,", humidity:",hum)
        data = [deviceiidd,temp,hum]
        if deviceiidd == "s3s9TFhT9WbDsA0CxlWeAKuZykjcmO6PoxK6" :
            with open('/home/pi/DJANGO/django_intro/s3s9TFhT9WbDsA0CxlWeAKuZykjcmO6PoxK6.cvs', 'a') as csvfile:
                fieldnames = ['deviceId', 'temperature', 'humidity', 'date' , 'time'  ]
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writerow({'deviceId': deviceiidd, 'temperature': temp, 'humidity': hum, 'date' : time.strftime('%m/%d/%y'), 'time' : time.strftime('%H:%M.%S') })
        if deviceiidd == "s3s9TFhT9WbDsA0CxlWeAKuZykjcmO6PoxK7" :
            with open('/home/pi/DJANGO/django_intro/s3s9TFhT9WbDsA0CxlWeAKuZykjcmO6PoxK7.cvs', 'a') as csvfile:
                fieldnames = ['deviceId', 'temperature', 'humidity', 'date' , 'time'  ]
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writerow({'deviceId': deviceiidd, 'temperature': temp, 'humidity': hum, 'date' : time.strftime('%m/%d/%y'), 'time' : time.strftime('%H:%M.%S') })
        #f = open('/home/pi/DJANGO/django_intro/plantilla33/datos2.cvs','w')
        #f.write(str(data))
        #f.close()



    client.subscribe(topic)
    client.on_message = on_message



def main():
    client = connect_mqtt()
    subscribe(client)

    client.loop_forever()

if __name__ == '__main__':
    main()
