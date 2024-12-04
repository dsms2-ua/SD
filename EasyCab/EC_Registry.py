from flask import Flask, request, jsonify
import requests

app = Flask(__name__)

def comprobarRegistro(id):
    #Comprobamos si el usuario ya está en la base de datos
    check = requests.get(f'http://localhost:3000/taxis/{id}')
    
    data = check.json()
    
    if data == "No se encontró el taxi":
        return False
    return True

@app.route('/registry', methods=['POST'])
def register():
    data = request.json
    id = data.get('id')
    
    #Comprobamos si el usuario ya está en la base de datos
    if not comprobarRegistro(id):
        #Lo insertamos en la base de datos
        requests.post('http://localhost:3000/taxis', json=data)
        return jsonify({"message": f"Taxi con id: {id} registrado correctamente."})
    else:
        return jsonify({"message": "El taxi ya está registrado."})
    
@app.route('/login', methods=['GET'])
def login():
    id = request.args.get('id')
    
    #Comprobamos si el usuario ya está en la base de datos
    #Si lo está, obtenemos su contraseña y la devolvemos
    if comprobarRegistro(id):
        return jsonify({"message": "Taxi logueado correctamente."})
    else:
        return jsonify({"message": "El taxi no está registrado."})
    
def exposeAPI():
    app.run(port=5000, debug=True)
    
def main():
    exposeAPI()
    
if __name__ == '__main__':
    main()


