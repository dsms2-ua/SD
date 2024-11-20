from kafka import KafkaProducer
import json
import time
import random
import requests
import sys
import threading

city="Madrid"
# Leer la ciudad desde el archivo JSON


def get_temperature():
    global city
    API_KEY = "fd9451beef3b8e622a3194365440b1dc"
    API_BASE_URL = "https://api.openweathermap.org/data/2.5/weather"
    
    params = {
        'q': city,
        'appid': API_KEY,
        'units': 'metric',  # Para recibir temperatura en °C
    }
    
    try:
        response = requests.get(API_BASE_URL, params=params)
        response.raise_for_status()  # Lanza excepción para códigos de error HTTP
        weather_data = response.json()
        return weather_data['main']['temp']  # Devuelve solo la temperatura
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        if response is not None:
            print("Mensaje del servidor:", response.json().get("message", "Error no especificado."))
    except Exception as err:
        print(f"Error occurred: {err}")
    return None

def send_temperature_updates():
    global city
    producer = KafkaProducer(bootstrap_servers=f'{sys.argv[1]}:{sys.argv[2]}')
    # Leer la ciudad desde el archivo
    print(f"Sending temperature updates for city: {city}")

    while True:
        # Generar una temperatura aleatoria para simular
        temperature = get_temperature()
        print(f"Publishing temperature: {temperature}°C")
        state = "OK" if temperature >=0 else "KO"
        # Enviar la temperatura al topic
        producer.send('weatherUpdate', state.encode('utf-8'))

        # Esperar 10 segundos antes de enviar la siguiente
        time.sleep(10)

#Pedimos al usuario si quiere cambiar la ciudad
def update_city():
    global city
    while True:
        input("Pulsa enter en cualquier momento para cambiar de ciudad:")
        city = input("Introduce la ciudad a la que te quieres cambiar: ")
        print(f"Actualizando ciudad a: {city}")

def main():
    if len(sys.argv) != 3:
        print("Uso: python EC_CTC.py <Bootstrap_IP> <Bootstrap_Port>")
        sys.exit(1)

    # Iniciar el hilo para enviar actualizaciones de temperatura
    temperature_thread = threading.Thread(target=send_temperature_updates)
    temperature_thread.start()

    city_update_thread = threading.Thread(target=update_city, daemon=True)
    city_update_thread.start()

    # Esperar a que los hilos terminen
    temperature_thread.join()
    city_update_thread.join()

if __name__ == "__main__":
    main()
