import json
import time
import requests
import sys
import threading

from flask import Flask, jsonify

city="Madrid"
# Leer la ciudad desde el archivo JSON
app = Flask(__name__)

def get_temperature():
    global city
    
    #La API de OpenWeather está en un archivo
    with open('APIOpenWeather.json', 'r') as file:
        API_KEY = json.load(file)['apiKey']
    
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

@app.route('/city', methods=['GET'])
def exposeAPI():
    temperatura = get_temperature()
    
    if temperatura < 0:
        return "KO"
    return "OK"

#Pedimos al usuario si quiere cambiar la ciudad
def update_city():
    global city
    while True:
        input("Pulsa enter en cualquier momento para cambiar de ciudad:")
        city = input("Introduce la ciudad a la que te quieres cambiar: ")
        print(f"Actualizando ciudad a: {city}")
        
def exposeAPI():
    app.run(debug=True, host='0.0.0.0', port=5000)

def main():
    #Creamos dos hilos, uno para cambiar la ciudad y otro para exponer la API
    hiloCiudad = threading.Thread(target=update_city)
    hiloCiudad.start()
    
    hiloAPI = threading.Thread(target=exposeAPI)
    hiloAPI.start()
    
    hiloCiudad.join()
    hiloAPI.join()

if __name__ == "__main__":
    main()
