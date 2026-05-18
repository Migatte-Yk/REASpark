# Análisis de Ventas con PySpark y Flask

Sistema de análisis de ventas que procesa 2 millones de registros usando Apache Spark como motor de cómputo distribuido y Flask como servidor web. Los resultados se presentan en un dashboard con tablas por ciudad, categoría y tienda.

## Tecnologías

- Python 3.8+
- Apache Spark 4.x (modo local)
- Flask 3.x
- Pandas
- Bootstrap 5

## Estructura del proyecto

```
REASpark/
    app.py                    # Servidor Flask y definición de rutas API
    spark.py                  # Sesión Spark y funciones de análisis
    requirements.txt          # Dependencias Python
    ventas_2_millones.csv     # Dataset de entrada (2 000 000 filas)
    templates/
        index.html            # Dashboard Bootstrap 5
```

## Requisitos previos

**Java 17** es obligatorio. PySpark no arranca sin la JVM.

En Windows se puede instalar con winget:

```
winget install Microsoft.OpenJDK.17
```

## Instalación local

```bash
# 1. Clonar o descargar el repositorio
# 2. Instalar dependencias
pip install -r requirements.txt
```

## Ejecución local

```bash
python app.py
```

La primera carga puede tardar entre 15 y 40 segundos mientras Spark lee y cachea el CSV. Una vez listo, el terminal muestra:

```
Datos cargados correctamente.
 * Running on http://127.0.0.1:5000
```

Abrir el navegador en `http://127.0.0.1:5000`.

## Rutas API

| Ruta              | Descripción                                      |
|-------------------|--------------------------------------------------|
| `GET /`           | Renderiza el dashboard                           |
| `GET /api/resumen`    | KPIs globales: total, transacciones, promedio |
| `GET /api/ciudades`   | Ventas agrupadas por ciudad                  |
| `GET /api/categorias` | Ventas agrupadas por categoría               |
| `GET /api/tiendas`    | Ventas agrupadas por tienda                  |

## Notas

- El DataFrame se cachea en memoria al arrancar. Las consultas posteriores son inmediatas.
- Spark UI disponible en `http://127.0.0.1:4040` mientras la app este corriendo (solo local).
- La advertencia `Did not find winutils.exe` en Windows es inofensiva y no afecta el funcionamiento.
