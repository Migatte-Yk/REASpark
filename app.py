import os
import time
from flask import Flask, render_template, jsonify
from spark import cargar_datos, ventas_por_ciudad, ventas_por_categoria, ventas_por_tienda, ventas_por_producto, resumen_general, get_spark_session

app = Flask(__name__)

RUTA_CSV = os.path.join(os.path.dirname(__file__), "ventas_2_millones.csv")

print("Cargando datos, esto puede tardar unos segundos...")
_df = cargar_datos(RUTA_CSV)
_df.cache()
print("Datos cargados correctamente.")


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/resumen")
def api_resumen():
    inicio = time.time()
    data = resumen_general(_df)
    data["tiempo_segundos"] = round(time.time() - inicio, 2)
    data["master"] = get_spark_session().sparkContext.master
    return jsonify(data)


@app.route("/api/ciudades")
def api_ciudades():
    return jsonify(ventas_por_ciudad(_df))


@app.route("/api/categorias")
def api_categorias():
    return jsonify(ventas_por_categoria(_df))


@app.route("/api/tiendas")
def api_tiendas():
    return jsonify(ventas_por_tienda(_df))


@app.route("/api/productos")
def api_productos():
    return jsonify(ventas_por_producto(_df))


if __name__ == "__main__":
    app.run(debug=True, use_reloader=False, host="0.0.0.0", port=5000)