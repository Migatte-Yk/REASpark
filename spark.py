import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ.setdefault("JAVA_HOME", r"C:\Program Files\Microsoft\jdk-17.0.19.10-hotspot")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, avg, count, round as spark_round

spark = None


def get_spark_session():
    global spark
    if spark is None:
        SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")
        spark = (
            SparkSession.builder
            .appName("AnalisisVentas")
            .master(SPARK_MASTER)
            .config("spark.driver.memory", "2g")
            .config("spark.executor.memory", "2g")
            .config("spark.ui.showConsoleProgress", "false")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")
        print(f"Spark corriendo en: {SPARK_MASTER}")
    return spark


def cargar_datos(ruta_csv: str):
    s = get_spark_session()
    if os.name == "nt" and os.getenv("SPARK_MASTER", "").startswith("spark://"):
        try:
            import pandas as pd
        except ImportError as err:
            raise RuntimeError("pandas es necesario para leer el CSV desde Windows cuando Spark corre en WSL") from err

        pdf = pd.read_csv(ruta_csv)
        df = s.createDataFrame(pdf)
    else:
        df = s.read.csv(ruta_csv, header=True, inferSchema=True)
    df = df.withColumn("total_venta", col("cantidad") * col("precio_unitario"))
    return df


def ventas_por_ciudad(df):
    resultado = (
        df.groupBy("ciudad")
        .agg(
            spark_sum("total_venta").alias("total_ventas"),
            count("id_venta").alias("num_transacciones"),
            spark_round(avg("total_venta"), 0).alias("promedio_venta")
        )
        .orderBy(col("total_ventas").desc())
    )
    return resultado.toPandas().to_dict(orient="records")


def ventas_por_categoria(df):
    resultado = (
        df.groupBy("categoria")
        .agg(
            spark_sum("total_venta").alias("total_ventas"),
            count("id_venta").alias("num_transacciones"),
            spark_round(avg("total_venta"), 0).alias("promedio_venta")
        )
        .orderBy(col("total_ventas").desc())
    )
    return resultado.toPandas().to_dict(orient="records")


def ventas_por_tienda(df):
    resultado = (
        df.groupBy("tienda")
        .agg(
            spark_sum("total_venta").alias("total_ventas"),
            count("id_venta").alias("num_transacciones"),
            spark_round(avg("total_venta"), 0).alias("promedio_venta")
        )
        .orderBy(col("total_ventas").desc())
    )
    return resultado.toPandas().to_dict(orient="records")


def ventas_por_producto(df, top_n=10):
    resultado = (
        df.groupBy("producto")
        .agg(
            spark_sum("total_venta").alias("total_ventas"),
            count("id_venta").alias("num_transacciones"),
            spark_round(avg("total_venta"), 0).alias("promedio_venta")
        )
        .orderBy(col("total_ventas").desc())
        .limit(top_n)
    )
    return resultado.toPandas().to_dict(orient="records")


def resumen_general(df):
    total = df.agg(
        spark_sum("total_venta").alias("gran_total"),
        count("id_venta").alias("total_transacciones"),
        spark_round(avg("total_venta"), 0).alias("promedio_general")
    ).collect()[0]
    return {
        "gran_total": int(total["gran_total"]),
        "total_transacciones": int(total["total_transacciones"]),
        "promedio_general": int(total["promedio_general"])
    }