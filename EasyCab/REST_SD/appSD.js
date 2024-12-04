const express = require("express");
const bodyParser = require("body-parser");
const sqlite3 = require("sqlite3");

const appSD = express();
// Se define el puerto
const port=3000;

const db = new sqlite3.Database('database.db', (error) => {
    if (error) {
        console.error("Error al conectar a la base de datos SQLite: ", error.message);
    } else {
        console.log("Conexión a la base de datos SQLite correcta");
    }
});

//Para poder procesar los parámetros dentro de body como json
appSD.use(bodyParser.json());

appSD.get("/",(req, res) => {
    res.json({message:'Página de inicio de aplicación de ejemplo de SD'})
});

// Ejecutar la aplicacion
appSD.listen(port, () => {
    console.log(`Ejecutando la aplicación API REST de SD en el puerto ${port}`);
});

// Listado de todos los usuarios
appSD.get("/usuarios",(request, response) => {
    console.log('Listado de todos los usuarios');
    const sql = 'SELECT * FROM Usuarios';
    db.all(sql, [], (err,resultado)=> {
        if (err) {
            res.status(500).send("Error obteniendo usuarios");
            console.error(err.message);
        } else{
            res.json(resultado)
        }
    });
});

// Listar todos los taxis
appSD.get("/taxis", (req, res) => {
    console.log('Listar todos los taxis');
    const sql = "SELECT * FROM Taxis";
    db.all(sql, [], (err, rows) => {
        if (err) {
            res.status(500).send("Error al obtener los taxis");
            console.error(err.message);
        } else {
            res.json(rows);
        }
    });
});

// Obtener un usuario por ID
appSD.get("/usuarios/:id", (req, res) => {
    const sql = "SELECT * FROM Usuarios WHERE idUsuario = ?";
    const params = [req.params.id];
    db.get(sql, params, (err, row) => {
        if (err) {
            res.status(500).send("Error al obtener el usuario");
            console.error(err.message);
        } else {
            res.json(row || "No se encontró el usuario");
        }
    });
});

//Obtener un taxi por ID
appSD.get("/taxis/:id", (req, res) => {
    console.log('Obtener taxi por ID');
    const sql = "SELECT * FROM Taxis WHERE idTaxi = ?";
    const params = [req.params.id];
    db.get(sql, params, (err, row) => {
        if (err) {
            res.status(500).send("Error al obtener el taxi");
            console.error(err.message);
        } else {
            res.json(row || "No se encontró el taxi");
        }
    });
});

// Agregar un nuevo usuario
appSD.post("/usuarios", (req, res) => {
    const { nombre, ciudad, correo } = req.body;
    const sql = "INSERT INTO Usuarios (nombre, ciudad, correo) VALUES (?, ?, ?)";
    const params = [nombre, ciudad, correo];
    db.run(sql, params, function (err) {
        if (err) {
            res.status(500).send("Error al crear el usuario");
            console.error(err.message);
        } else {
            res.send(`Usuario creado con ID: ${this.lastID}`);
        }
    });
});

//Agregar un nuevo taxi
appSD.post("/taxis", (req, res) => {
    console.log('Crear taxi');
    const { id, password } = req.body;
    const sql = "INSERT INTO Taxis (idTaxi, password) VALUES (?, ?)";
    const params = [id, password];
    db.run(sql, params, function (err) {
        if (err) {
            res.status(500).send("Error al crear el taxi");
            console.error(err.message);
        }
        else {
            res.send(`Taxi creado con ID: ${this.lastID}`);
            console.log(`Taxi creado con ID: ${this.lastID}`);
        }
    });
});

// Actualizar un usuario
appSD.put("/usuarios/:id", (req, res) => {
    const { nombre, ciudad, correo } = req.body;
    const sql = `
        UPDATE Usuarios 
        SET nombre = ?, ciudad = ?, correo = ? 
        WHERE idUsuario = ?`;
    const params = [nombre, ciudad, correo, req.params.id];
    db.run(sql, params, function (err) {
        if (err) {
            res.status(500).send("Error al actualizar el usuario");
            console.error(err.message);
        } else {
            res.send("Usuario actualizado correctamente");
        }
    });
});

// Modificar un usuario
appSD.put("/usuarios/:id",(request, response) => {
    console.log('Modificar usuario');
    const {id} = request.params;
    const {nombre,ciudad,correo} = request.body;
    const sql = `UPDATE Usuarios SET nombre='${nombre}', ciudad='${ciudad}', correo='${correo}' WHERE idUsuario=${id}`;
    connection.query(sql,error => {
        if (error) throw error;
        response.send('Usuario modificado');
        });
});

// Modificar un taxi
// TODO: Añadir los campos que faltan
appSD.put("/taxis/:id",(request, response) => {
    console.log('Modificar taxi');
    const {id} = request.params;
    const {clave,token,correo} = request.body;
    const sql = `UPDATE Taxis SET clave='${nombre}', token='${token}', correo='${correo}' WHERE idUsuario=${id}`;
    connection.query(sql,error => {
        if (error) throw error;
        response.send('Taxi modificado');
        });
});

// Borrar un usuario
appSD.delete("/usuarios/:id",(request, response) => {
    console.log('Borrar usuario');
    const {id} = request.params;
    sql = `DELETE FROM Usuarios WHERE idUsuario= ${id}`;
    connection.query(sql,error => {
        if (error) throw error;
        response.send('Usuario borrado');
        });
});

// Borrar un taxi
appSD.delete("/taxis/:id",(req, res) => {
    console.log('Borrar taxi');
    const { id } = req.body;
    const sql = "DELETE FROM TAXIS WHERE idTaxi = ?";
    const params = [id];
    db.run(sql, params, function (err) {
        if (err) {
            res.status(500).send("Error al eliminar el taxi");
            console.error(err.message);
        }
        else {
            res.send(`Taxi eliminado con ID: ${this.lastID}`);
            console.log(`Taxi eliminado con ID: ${this.lastID}`);
        }
    });
});

// Recuperar contraseña usuario HASH
appSD.get("/password/:id",(req, res) => {
    console.log('Recuperar contraseña usuario');
    const sql = `SELECT password FROM Taxis WHERE idTaxi= ?`;
    const params = [req.params.id];
    db.get(sql, params, (err, row) => {
        if (err) {
            res.status(500).send("Error al obtener la contraseña");
            console.error(err.message);
        } else {
            res.json(row || "No se encontró la contraseña");
        }
    });
});

// Leer fichero JSON
appSD.get("/usuarios_json",(request, response) => {
    console.log('Leer datos de fichero en formato JSON');
    try {
        const datos = fs.readFileSync('usuarios.json','utf8');
        response.send(JSON.parse(datos));
        } catch (error) {
        console.log (error);
        }
});

// Mandar el mapa al front