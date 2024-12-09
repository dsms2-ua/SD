from flask import Flask, jsonify
import re, os

app = Flask(__name__)

def leer_mapa():
    print(os.getcwd())
    # El mapa está en un archivo de texto
    ruta_archivo = os.path.join(os.path.dirname(__file__), 'mapa.txt')
    with open(ruta_archivo, 'r') as file:
        return file.readlines()
    
ANSI_COLORS = {
    '30': 'blue', '31': 'red', '32': 'green', '33': 'yellow', '34': 'blue', '35': 'magenta',
    '36': 'cyan', '37': 'white', '90': 'gray', '91': 'lightred', '92': 'lightgreen',
    '93': 'lightyellow', '94': 'lightblue', '95': 'lightmagenta', '96': 'lightcyan', '97': 'lightwhite'
}
    
def ansi_to_html(ansi_string):
    """
    Convierte una cadena de texto con códigos ANSI a HTML.
    """
    # Expresión regular para encontrar las secuencias ANSI
    ansi_escape = re.compile(r'\033\[(\d+)(?:;(\d+))?m')

    result = ""
    pos = 0  # Posición de lectura dentro de la cadena
    while pos < len(ansi_string):
        # Busca el siguiente código ANSI
        match = ansi_escape.match(ansi_string, pos)
        if match:
            pos = match.end()  # Avanzamos hasta después del código ANSI
            
            
        else:
            # Si no hay código ANSI, simplemente agregamos el texto
            char = ansi_string[pos]
            if char == '\n':
                result += " "  # Convertir salto de línea a un espacio en blanco
            else:
                result += char
            pos += 1
    
    # Asegurarnos de cerrar todas las etiquetas <p> abiertas
    result = result.replace("</p>", "").strip()
    
    # Convertir las secuencias de salto de línea a un espacio de forma más controlada
    result = result.replace("\n", " ")

    # Devolvemos el texto convertido a HTML
    return result

@app.route('/actualizarMapa')
def mostrar_mapa():
    try:
        # Leer el mapa (con ANSI)
        mapa = leer_mapa()
        
        mapahtml = [ansi_to_html(line) for line in mapa]
        
        # Retornar el mapa en formato JSON, asegurando que sea una cadena HTML
        return jsonify({'mapa': mapahtml})

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
