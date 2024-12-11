from flask import Flask, request, jsonify
import requests

app = Flask(__name__)

def comprobarRegistro(id):
    #Comprobamos si el usuario ya está en la base de datos
    print("Comprobando registro...")
    check = requests.get(f'http://localhost:3000/taxis/{id}')
    
    if check.status_code == 200:
        return True
    else:
        return False

@app.route('/registry', methods=['POST'])
def register():
    print("Registrando taxi...")
    data = request.json
    id = data.get('id')
    
    #Comprobamos si el usuario ya está en la base de datos
    if not comprobarRegistro(id):
        print("Registrando taxi...")
        
        
        #Lo insertamos en la base de datos
        response = requests.post('http://localhost:3000/taxis', json=data)
        if response.status_code == 200:
            return jsonify({"message": f"Taxi con id: {id} registrado correctamente."}), 200
        else:
            return jsonify({"message": "Error al registrar el taxi."}), 500
    else:
        return jsonify({"message": "El taxi ya está registrado."}), 404
    
@app.route('/delete', methods=['DELETE'])
def delete():
    print("Borrando taxi...")
    data = request.json
    id = data.get('id')
    
    #Comprobamos si el usuario ya está en la base de datos
    if comprobarRegistro(id):
        print("Borrando taxi...")
        
        #Lo eliminamos de la base de datos
        response = requests.delete(f'http://localhost:3000/taxis/{id}')
        
        if response.status_code == 200:
            return jsonify({"message": f"Taxi con id: {id} eliminado correctamente."}), 200
        else:
            return jsonify({"message": "Error al eliminar el taxi."}), 500
    else:
        return jsonify({"message": "El taxi no está registrado."}), 404
        
    
    
def exposeAPI():
    #Exponemos la API  
    context = ('certRegistrySans.pem', 'keyRegistrySans.pem')
    app.run(port=3003, debug=True)
    
def main():
    exposeAPI()
    
if __name__ == '__main__':
    main()


