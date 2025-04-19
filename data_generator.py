import requests
import math
import random
import datetime

DEVICE_ID = "PO1"
FLASK_SERVER = "http://127.0.0.1:5000/temperature"

def request_generator():
    random_temperature = 0
    data = {}
    print("initiating temp readings ... ")
    #'device_id', 'temperature', 'client_timestamp'
    i = 0
    while i<1200:
        i += 1
        random_temperature = round(random.uniform(1.0, 30.0), 2)
        data = {
            "device_id" : DEVICE_ID,
            "temperature" : random_temperature,
            "client_timestamp" : datetime.datetime.now().strftime("%d/%m/%Y, %H:%M:%S")    
        }
        headers = {'Content-type' : 'application/json'}
        try:
            response = requests.post(url=FLASK_SERVER, json=data, headers=headers) 
            print(f"response code = {response.status_code}")
        except Exception as ex:
            print(f"unexpected error = {ex}")

if __name__ == "__main__":
    request_generator()