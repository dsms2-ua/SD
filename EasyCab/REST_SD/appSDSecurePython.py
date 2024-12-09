from flask import Flask, request, jsonify
import sqlite3
import ssl

# Definimos el puerto
port = 3000

# Creamos la aplicación
appSD = Flask(__name__)

# Conexión a la base de datos
def get_db_connection():
    conn = sqlite3.connect('database.db')
    conn.row_factory = sqlite3.Row
    return conn

# Listado de todos los taxis
@appSD.route("/taxis", methods=['GET'])
def listar_taxis():
    print('Listar todos los taxis')
    conn = get_db_connection()
    taxis = conn.execute("SELECT * FROM Taxis").fetchall()
    conn.close()
    return jsonify([dict(row) for row in taxis])

# Obtener un taxi por ID
@appSD.route("/taxi/<int:id>", methods=['GET'])
def obtener_taxi(id):
    print('Obtener taxi por ID')
    conn = get_db_connection()
    taxi = conn.execute("SELECT * FROM Taxis WHERE idTaxi = ?", (id,)).fetchone()
    conn.close()
    if taxi is None:
        return jsonify({"message": "No se encontró el taxi"}), 404
    return jsonify(dict(taxi))

# Agregar un nuevo taxi
@appSD.route("/taxis", methods=['POST'])
def crear_taxi():
    print('Crear taxi')
    data = request.json
    id = data.get('id')
    password = data.get('password')
    conn = get_db_connection()
    try:
        conn.execute("INSERT INTO Taxis (idTaxi, password) VALUES (?, ?)", (id, password))
        conn.commit()
        conn.close()
        return jsonify({"message": f"Taxi creado con ID: {id}"}), 201
    except sqlite3.Error as e:
        conn.close()
        return jsonify({"message": "Error al crear el taxi", "error": str(e)}), 500

# Borrar un taxi
@appSD.route("/taxis/<int:id>", methods=['DELETE'])
def borrar_taxi(id):
    print('Borrar taxi')
    conn = get_db_connection()
    try:
        conn.execute("DELETE FROM Taxis WHERE idTaxi = ?", (id,))
        conn.commit()
        conn.close()
        return jsonify({"message": f"Taxi eliminado con ID: {id}"})
    except sqlite3.Error as e:
        conn.close()
        return jsonify({"message": "Error al eliminar el taxi", "error": str(e)}), 500

# Recuperar contraseña taxi con HASH
@appSD.route("/password/<int:id>", methods=['GET'])
def recuperar_password(id):
    print('Recuperar contraseña taxi')
    conn = get_db_connection()
    password = conn.execute("SELECT password FROM Taxis WHERE idTaxi = ?", (id,)).fetchone()
    conn.close()
    if password is None:
        return jsonify({"message": "No se encontró el taxi"}), 404
    return jsonify(dict(password))

if __name__ == '__main__':
    context = ('certAppSD.pem', 'keyAppSD.pem')  # Ruta a tus archivos de certificado y clave
    appSD.run(port=port, ssl_context=context, debug=True)