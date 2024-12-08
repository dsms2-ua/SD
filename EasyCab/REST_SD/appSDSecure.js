const https = require('https');
const express = require("express");
const fs = require('fs');
const bodyParser = require("body-parser");
const sqlite3 = require("sqlite3");

// Definimos el puerto
const port = 3000;

// Creamos la aplicación
const appSD = express();

appSD.use(bodyParser.json());

// Conexión a la base de datos
const db = new sqlite3.Database('database.db', (error) => {
    if (error) {
        console.error("Error al conectar a la base de datos SQLite: ", error.message);
    } else {
        console.log("Conexión a la base de datos SQLite correcta");
    }
});

// Verificar la carga de los certificados
let key, cert;
try {
    key = fs.readFileSync('keyAppSD.pem');
    cert = fs.readFileSync('certAppSD.pem');
    console.log("Certificados cargados correctamente");
} catch (error) {
    console.error("Error al cargar los certificados:", error.message);
    process.exit(1); // Salir si no se pueden cargar los certificados
}

// Arrancamos el servidor
https
    .createServer(
        // Indicamos el certificado y la clave privada
        {
            key: key,
            cert: cert
        },
        appSD
    )
    .listen(port, () => {
        console.log(`Ejecutando la aplicación API REST de SD en el puerto ${port}`);
    });

// Listado de todos los taxis
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

//Obtener un taxi por ID
appSD.get("/taxi/:id", (req, res) => {
    console.log('Obtener taxi por ID');
    const sql = "SELECT * FROM Taxis WHERE idTaxi = ?";
    const params = [req.params.id];
    db.get(sql, params, (err, row) => {
        if (err) {
            res.status(500).send("Error al obtener el taxi");
            console.error(err.message);
        }
        else if (!row) {
            res.status(404).json({ message: "No se encontró el taxi" });
        }
        else {
            res.json(row || "No se encontró el taxi");
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

// Recuperar contraseña taxi con HASH
appSD.get("/password/:id",(req, res) => {
    console.log('Recuperar contraseña taxi');
    const sql = `SELECT password FROM Taxis WHERE idTaxi= ?`;
    const params = [req.params.id];
    db.get(sql, params, (err, row) => {
        if (err) {
            res.status(500).send("Error al obtener la contraseña");
            console.error(err.message);
        } 
        else if (!row) {
            res.status(404).json({ message: "No se encontró el taxi" });
        }
        else {
            res.json(row || "No se encontró la contraseña");
        }
    });
});