from flask import Flask, request, jsonify
import sqlite3

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
        result = conn.execute("DELETE FROM Taxis WHERE idTaxi = ?", (id,))
        conn.commit()
        conn.close()
        if result.rowcount == 0:
            return jsonify({"message": "No se encontró el taxi"}), 404
        return jsonify({"message": f"Taxi eliminado con ID: {id}"}), 200
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

# Recuperar token de un taxi con id
@appSD.route("/token/<int:id>", methods=['GET'])
def recuperar_token(id):
    print('Recuperar token taxi')
    conn = get_db_connection()
    token = conn.execute("SELECT token FROM Taxis WHERE idTaxi = ?", (id,)).fetchone()
    conn.close()
    if token is None:
        return jsonify({"message": "No se encontró el taxi"}), 404
    return jsonify(dict(token))

# Recuperar aes de un taxi con id
@appSD.route("/aes/<int:id>", methods=['GET'])
def recuperar_aes(id):
    print('Recuperar aes taxi')
    conn = get_db_connection()
    aes = conn.execute("SELECT aes FROM Taxis WHERE idTaxi = ?", (id,)).fetchone()
    conn.close()
    if aes is None:
        return jsonify({"message": "No se encontró el taxi"}), 404
    return jsonify(dict(aes))

# Recuperar aes de un taxi con token
@appSD.route("/aes/token/<string:token>", methods=['GET'])
def recuperar_aes_por_token(token):
    print('Recuperar aes taxi')
    conn = get_db_connection()
    aes = conn.execute("SELECT aes FROM Taxis WHERE token = ?", (token,)).fetchone()
    conn.close()
    if aes is None:
        return jsonify({"message": "No se encontró el taxi"}), 404
    return jsonify(dict(aes))

# Actualizar token de un taxi
@appSD.route("/token/<int:id>", methods=['PUT'])
def actualizar_token(id):
    print('Actualizar token taxi')
    data = request.json
    token = data.get('token')
    conn = get_db_connection()
    try:
        conn.execute("UPDATE Taxis SET token = ? WHERE idTaxi = ?", (token, id))
        conn.commit()
        conn.close()
        return jsonify({"message": f"Token actualizado con ID: {id}"}), 200
    except sqlite3.Error as e:
        conn.close()
        return jsonify({"message": "Error al actualizar el token", "error": str(e)}), 500

# Actualizar aes de un taxi
@appSD.route("/aes/<int:id>", methods=['PUT'])
def actualizar_aes(id):
    print('Actualizar aes taxi')
    data = request.json
    aes = data.get('aes')
    conn = get_db_connection()
    try:
        conn.execute("UPDATE Taxis SET aes = ? WHERE idTaxi = ?", (aes, id))
        conn.commit()
        conn.close()
        return jsonify({"message": f"AES actualizado con ID: {id}"}), 200
    except sqlite3.Error as e:
        conn.close()
        return jsonify({"message": "Error al actualizar el aes", "error": str(e)}), 500

# Recuperar id de un taxi con token
@appSD.route("/id/token/<string:token>", methods=['GET'])
def recuperar_id_por_token(token):
    print('Recuperar id taxi')
    conn = get_db_connection()
    id_taxi = conn.execute("SELECT idTaxi FROM Taxis WHERE token = ?", (token,)).fetchone()
    conn.close()
    if id_taxi is None:
        return jsonify({"message": "No se encontró el taxi"}), 404
    return jsonify(dict(id_taxi))

if __name__ == '__main__':
    appSD.run(port=port, debug=True)