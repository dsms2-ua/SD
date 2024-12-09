from flask import Flask, jsonify
import re, os

app = Flask(__name__)

def leer_mapa():
    print(os.getcwd())
    # El mapa está en un archivo de texto
    ruta_archivo = os.path.join(os.path.dirname(__file__), 'mapa.txt')
    with open(ruta_archivo, 'r') as file:
        return file.readlines()

@app.route('/actualizarMapa')
def mostrar_mapa():
    try:
        # Leer el mapa (con ANSI)
        mapa = leer_mapa()
        
        # Retornar el mapa en formato JSON, asegurando que sea una cadena HTML
        return jsonify({'mapa': mapa})

    except Exception as e:
        # Si ocurre un error, devolver un mensaje de error
        return jsonify({'error': str(e)}), 500

@app.route('/mapa')
def pagina_principal():
    # Renderiza la página HTML que carga dinámicamente el mapa
    html_template = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Mapa Dinámico</title>
        <style>
            pre {{
                font-size: 16px;
                font-family: monospace;
                text-align: center;
                white-space: pre-wrap; /* Esto asegura que el texto largo se ajuste */
            }}
        </style>
    </head>
    <body>
        <h1>Mapa Dinámico</h1>
        <pre id="mapa">Cargando mapa...</pre>
        <script>
            // Actualiza el mapa cada segundo
            setInterval(async function() {{
                const response = await fetch('/actualizarMapa'); // Solicita el mapa al servidor
                const data = await response.json();   // Convierte la respuesta a JSON
                document.getElementById('mapa').textContent = data.mapa.join('\\n'); // Actualiza el contenido
            }}, 1000); // Cada 1000ms (1 segundo)
        </script>
    </body>
    </html>
    """
    return html_template

if __name__ == '__main__':
    app.run(debug=True, port=3001)
