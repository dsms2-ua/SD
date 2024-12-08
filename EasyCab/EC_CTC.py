import json
from flask import Flask, jsonify
import requests
import logging
import threading

app = Flask(__name__)
city = "Madrid"  # Ciudad inicial

# Configurar Flask para que no emita logs en la consola
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

def get_temperature():
    global city

    # Leer la API desde el archivo
    try:
        with open('APIOpenWeather.json', 'r') as file:
            API_KEY = json.load(file)['apiKey']
    except FileNotFoundError:
        print("Error: Archivo APIOpenWeather.json no encontrado.")
        return None

    API_BASE_URL = "https://api.openweathermap.org/data/2.5/weather"

    params = {
        'q': city,
        'appid': API_KEY,
        'units': 'metric',  # Para recibir temperatura en °C
    }

    try:
        response = requests.get(API_BASE_URL, params=params)
        response.raise_for_status()
        weather_data = response.json()
        return weather_data['main']['temp']  # Devuelve solo la temperatura
    except requests.exceptions.HTTPError as http_err:
        print(f"Error HTTP: {http_err}")
        if response is not None:
            print("Mensaje del servidor:", response.json().get("message", "Error no especificado."))
    except Exception as err:
        print(f"Error general: {err}")
    return None

@app.route('/city', methods=['GET'])
def get_city_temperature():
    temperatura = get_temperature()
    if temperatura is None or temperatura < 0:
        status = 'KO'
    else:
        status = 'OK'
    return jsonify({'city': city, 'temperature': temperatura, 'status': status})

def update_city():
    global city
    while True:
        # Solicitar nueva ciudad
        new_city = input("Introduce la nueva ciudad (o presiona Enter para mantener la actual): ").strip()
        if new_city:
            city = new_city  # Actualizar la ciudad
            print(f"Ciudad actualizada a: {city}")

def main():
    # Hilo para actualizar la ciudad
    hiloCiudad = threading.Thread(target=update_city, daemon=True)
    hiloCiudad.start()

    # Ejecutar Flask en el hilo principal
    context = ('certificados/certCTC.pem', 'certificados/keyCTC.pem')  # Ajusta los certificados si es necesario
    app.run(port=3002, debug=False)#, ssl_context=context)

if __name__ == '__main__':
    main()
