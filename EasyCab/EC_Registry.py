from flask import Flask, request, jsonify
import requests

app = Flask(__name__)

def comprobarRegistro(id):
    #Comprobamos si el usuario ya está en la base de datos
    print("Comprobando registro...")
    check = requests.get(f'https://localhost:3000/taxis/{id}', verify='certificados/certAppSD.pem')
    
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
        requests.post('https://localhost:3000/taxis', json=data, verify='REST_SD/certAppSD.pem')
        return jsonify({"message": f"Taxi con id: {id} registrado correctamente."})
    else:
        return jsonify({"message": "El taxi ya está registrado."})
    
    
def exposeAPI():
    #Exponemos la API con https
    context = ('certificados/certRegistry.pem', 'certificados/keyRegistry.pem')   
    app.run(port=3003, debug=True, ssl_context=context)
    
def main():
    exposeAPI()
    
if __name__ == '__main__':
    main()


