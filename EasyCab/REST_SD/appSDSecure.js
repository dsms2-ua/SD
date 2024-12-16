const express = require("express");
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

appSD.listen(port, '0.0.0.0', () => {
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
    const sql = "DELETE FROM TAXIS WHERE idTaxi = ?";
    id = req.params.id;
    const params = [id];
    db.run(sql, params, function (err) {
        if (err) {
            res.status(500).send("Error al eliminar el taxi");
            console.error(err.message);
        }
        //En principio esta opcion no se da porque hacemos una comprobacion previa
        else if (this.changes === 0) {
            res.status(404).json({ message: "No se encontró el taxi" });
        }
        else {
            res.status(200).send(`Taxi eliminado con ID: ${id}`);
            console.log(`Taxi eliminado con ID: ${id}`);
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

//recuperar token de un taxi con id
appSD.get("/token/:id",(req, res) => {
    console.log('Recuperar token taxi');
    const sql = `SELECT token FROM Taxis WHERE idTaxi= ?`;
    const params = [req.params.id];
    db.get(sql, params, (err, row) => {
        if (err) {
            res.status(500).send("Error al obtener el token");
            console.error(err.message);
        } 
        else if (!row) {
            res.status(404).json({ message: "No se encontró el taxi" });
        }
        else {
            res.json(row || "No se encontró el token");
        }
    });
});


//recuperar aes de un taxi con id
appSD.get("/aes/:id",(req, res) => {
    console.log('Recuperar aes taxi');
    const sql = `SELECT aes FROM Taxis WHERE idTaxi= ?`;
    const params = [req.params.id];
    db.get(sql, params, (err, row) => {
        if (err) {
            res.status(500).send("Error al obtener el aes");
            console.error(err.message);
        } 
        else if (!row) {
            res.status(404).json({ message: "No se encontró el taxi" });
        }
        else {
            res.json(row || "No se encontró el aes");
        }
    });
});

//recuperar aes de un taxi con token
appSD.get("/aes/token/:token",(req, res) => {
    console.log('Recuperar aes taxi');
    const sql = `SELECT aes FROM Taxis WHERE token= ?`;
    const params = [req.params.token];
    db.get(sql, params, (err, row) => {
        if (err) {
            res.status(500).send("Error al obtener el aes");
            console.error(err.message);
        } 
        else if (!row) {
            res.status(404).json({ message: "No se encontró el taxi" });
        }
        else {
            res.json(row || "No se encontró el aes");
        }
    });
});

//Actualizar token de un taxi
appSD.put("/token/:id",(req, res) => {
    console.log('Actualizar token taxi');
    const { token } = req.body;
    const sql = `UPDATE Taxis SET token = ? WHERE idTaxi = ?`;
    const params = [token, req.params.id];
    db.run(sql, params, function (err) {
        if (err) {
            res.status(500).send("Error al actualizar el token");
            console.error(err.message);
        }
        else {
            res.send(`Token actualizado con ID: ${req.params.id}`);
            console.log(`Token actualizado con ID: ${req.params.id}`);
        }
    });
});

//Actualizar aes de un taxi
appSD.put("/aes/:id",(req, res) => {
    console.log('Actualizar aes taxi');
    const { aes } = req.body;
    const sql = `UPDATE Taxis SET aes = ? WHERE idTaxi = ?`;
    const params = [aes, req.params.id];
    db.run(sql, params, function (err) {
        if (err) {
            res.status(500).send("Error al actualizar el aes");
            console.error(err.message);
        }
        else {
            res.send(`AES actualizado con ID: ${req.params.id}`);
            console.log(`AES actualizado con ID: ${req.params.id}`);
        }
    });
});

// Recuperar id de un taxi con token
appSD.get("/id/token/:token",(req, res) => {
    console.log('Recuperar id taxi');
    const sql = `SELECT idTaxi FROM Taxis WHERE token= ?`;
    const params = [req.params.token];
    db.get(sql, params, (err, row) => {
        if (err) {
            res.status(500).send("Error al obtener el id");
            console.error(err.message);
        } 
        else if (!row) {
            res.status(404).json({ message: "No se encontró el taxi" });
        }
        else {
            res.json(row || "No se encontró el id");
        }
    });
});

