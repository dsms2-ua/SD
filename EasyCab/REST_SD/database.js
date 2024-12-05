const sqlite3 = require('sqlite3').verbose();

// Crear una conexión a la base de datos (se crea automáticamente si no existe)
const db = new sqlite3.Database('database.db', (err) => {
    if (err) {
        console.error('Error al conectar a la base de datos:', err.message);
    } else {
        console.log('Conectado a la base de datos SQLite');
    }
});

module.exports = db;

// Crear la tabla de usuarios
const crearTabla = `
CREATE TABLE IF NOT EXISTS Taxis (
    idTaxi INTEGER PRIMARY KEY,
    password TEXT NOT NULL,
    token TEXT
);
`;

borrar = `DROP TABLE Taxis`;

db.run(crearTabla, (err) => {
    if (err) {
        console.error('Error al crear la tabla:', err.message);
    } else {
        console.log('Tabla creada exitosamente.');
    }
});

db.close();