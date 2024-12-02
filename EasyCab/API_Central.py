#Aquí expondremos una API donde podremos consultar el estado de los taxis, usuarios y el mapa

from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route('/taxis', methods=['GET'])

if __name__ == '__main__':
    app.run(port=5002, debug=True)