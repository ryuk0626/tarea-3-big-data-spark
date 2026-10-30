import os

# Indicar a Spark dónde está instalado Python
ruta_python = (
    r"C:\Users\EATC-\AppData\Local\Python"
    r"\pythoncore-3.14-64\python.exe"
)

os.environ["PYSPARK_PYTHON"] = ruta_python
os.environ["PYSPARK_DRIVER_PYTHON"] = ruta_python

# Importar Spark y las funciones necesarias
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    when,
    sum,
    to_date,
    year,
    month
)

# Crear la sesión de Spark
spark = (
    SparkSession.builder
    .appName("AccidentesTransito")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# Nombre del archivo CSV
archivo = "LESIONES_ACCIDENTES_DE_TRÁNSITO_COLOMBIA.csv"

# Cargar el archivo
datos = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(archivo)
)

# Mostrar los primeros registros
print("\nPRIMEROS CINCO REGISTROS:")

datos.show(5, truncate=False)

# Mostrar las columnas
print("\nCOLUMNAS DEL ARCHIVO:")

for columna in datos.columns:
    print("-", columna)

# Contar los registros
print("\nTOTAL DE REGISTROS:")

total_registros = datos.count()

print(total_registros)

# Mostrar los tipos iniciales
print("\nTIPOS DE DATOS INICIALES:")

datos.printSchema()

# Revisar valores vacíos
print("\nVALORES VACIOS POR COLUMNA:")

datos.select([
    sum(
        when(
            col(c).isNull() | (trim(col(c)) == ""),
            1
        ).otherwise(0)
    ).alias(c)
    for c in datos.columns
]).show(truncate=False)

# Convertir fecha y cantidad
print("\nCONVERSION DE DATOS:")

datos_limpios = (
    datos
    .withColumn(
        "FECHA",
        to_date(col("FECHA HECHO"), "dd/MM/yyyy")
    )
    .withColumn(
        "CANTIDAD_NUMERICA",
        col("CANTIDAD").cast("integer")
    )
    .withColumn(
        "ANIO",
        year(col("FECHA"))
    )
    .withColumn(
        "MES",
        month(col("FECHA"))
    )
)

# Mostrar los datos convertidos
datos_limpios.select(
    "FECHA",
    "ANIO",
    "MES",
    "DEPARTAMENTO",
    "MUNICIPIO",
    "CANTIDAD_NUMERICA"
).show(5, truncate=False)

# Mostrar los nuevos tipos
print("\nNUEVOS TIPOS DE DATOS:")

datos_limpios.select(
    "FECHA",
    "ANIO",
    "MES",
    "CANTIDAD_NUMERICA"
).printSchema()

# Comprobar conversiones incorrectas
print("\nREVISION DE CONVERSIONES:")

fechas_incorrectas = datos_limpios.filter(
    col("FECHA").isNull()
).count()

cantidades_incorrectas = datos_limpios.filter(
    col("CANTIDAD_NUMERICA").isNull()
).count()

print("Fechas incorrectas:", fechas_incorrectas)
print("Cantidades incorrectas:", cantidades_incorrectas)

# Revisar registros con exactamente los mismos valores
print("\nREVISION DE REGISTROS IGUALES:")

registros_totales = datos.count()
registros_distintos = datos.distinct().count()

registros_iguales = (
    registros_totales - registros_distintos
)

print("Registros totales:", registros_totales)
print("Registros distintos:", registros_distintos)
print("Registros aparentemente repetidos:", registros_iguales)
print("Los registros se conservaran porque no existe un identificador unico.")
# Obtener estadísticas generales
print("\nESTADISTICAS GENERALES:")

from pyspark.sql.functions import avg, min, max

estadisticas = datos_limpios.select(
    sum("CANTIDAD_NUMERICA").alias("TOTAL_LESIONADOS"),
    avg("CANTIDAD_NUMERICA").alias("PROMEDIO"),
    min("CANTIDAD_NUMERICA").alias("MINIMO"),
    max("CANTIDAD_NUMERICA").alias("MAXIMO")
)

estadisticas.show(truncate=False)
# Calcular los 10 departamentos con mayor cantidad
print("\nDIEZ DEPARTAMENTOS CON MAYOR CANTIDAD:")

departamentos_mayores = (
    datos_limpios
    .groupBy("DEPARTAMENTO")
    .sum("CANTIDAD_NUMERICA")
    .withColumnRenamed(
        "sum(CANTIDAD_NUMERICA)",
        "TOTAL_LESIONADOS"
    )
    .orderBy(
        col("TOTAL_LESIONADOS").desc()
    )
)

departamentos_mayores.show(10, truncate=False)
# Calcular los 10 municipios con mayor cantidad
print("\nDIEZ MUNICIPIOS CON MAYOR CANTIDAD:")

municipios_mayores = (
    datos_limpios
    .groupBy("DEPARTAMENTO", "MUNICIPIO")
    .sum("CANTIDAD_NUMERICA")
    .withColumnRenamed(
        "sum(CANTIDAD_NUMERICA)",
        "TOTAL_LESIONADOS"
    )
    .orderBy(
        col("TOTAL_LESIONADOS").desc()
    )
)

municipios_mayores.show(10, truncate=False)
# Calcular la cantidad registrada por año
print("\nCANTIDAD DE LESIONADOS POR AÑO:")

resultados_anuales = (
    datos_limpios
    .groupBy("ANIO")
    .sum("CANTIDAD_NUMERICA")
    .withColumnRenamed(
        "sum(CANTIDAD_NUMERICA)",
        "TOTAL_LESIONADOS"
    )
    .orderBy("ANIO")
)

resultados_anuales.show(30,truncate=False)
# Calcular la cantidad registrada por año y mes
# Calcular la cantidad total por mes
print("\nCANTIDAD TOTAL DE LESIONADOS POR MES:")

resultados_mensuales = (
    datos_limpios
    .groupBy("MES")
    .sum("CANTIDAD_NUMERICA")
    .withColumnRenamed(
        "sum(CANTIDAD_NUMERICA)",
        "TOTAL_LESIONADOS"
    )
    .orderBy("MES")
)

resultados_mensuales.show(12, truncate=False)
# Crear una gráfica con los 10 departamentos principales
print("\nCREANDO GRAFICA DE DEPARTAMENTOS...")

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt

# Obtener los 10 resultados calculados con Spark
filas_departamentos = departamentos_mayores.limit(10).collect()

nombres_departamentos = [
    fila["DEPARTAMENTO"]
    for fila in filas_departamentos
]

totales_departamentos = [
    fila["TOTAL_LESIONADOS"]
    for fila in filas_departamentos
]

# Crear una gráfica horizontal
plt.figure(figsize=(11, 6))

plt.barh(
    nombres_departamentos,
    totales_departamentos,
    color="steelblue"
)

plt.title(
    "Diez departamentos con mayor cantidad de personas lesionadas"
)

plt.xlabel("Cantidad de personas lesionadas")
plt.ylabel("Departamento")

# Mostrar el mayor resultado en la parte superior
plt.gca().invert_yaxis()

# Añadir el valor al final de cada barra
for posicion, valor in enumerate(totales_departamentos):
    plt.text(
        valor,
        posicion,
        f" {valor:,}".replace(",", "."),
        va="center"
    )

plt.tight_layout()

# Guardar la gráfica en la carpeta del proyecto
ruta_grafica_departamentos = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "grafica_departamentos.png"
)

plt.savefig(
    ruta_grafica_departamentos,
    dpi=300,
    bbox_inches="tight"
)

plt.close()

print("Grafica guardada correctamente en:")
print(ruta_grafica_departamentos)
# Crear una gráfica con los 10 municipios principales
print("\nCREANDO GRAFICA DE MUNICIPIOS...")

filas_municipios = municipios_mayores.limit(10).collect()

nombres_municipios = [
    fila["MUNICIPIO"]
    for fila in filas_municipios
]

totales_municipios = [
    fila["TOTAL_LESIONADOS"]
    for fila in filas_municipios
]

plt.figure(figsize=(11, 6))

plt.barh(
    nombres_municipios,
    totales_municipios,
    color="darkorange"
)

plt.title(
    "Diez municipios con mayor cantidad de personas lesionadas"
)

plt.xlabel("Cantidad de personas lesionadas")
plt.ylabel("Municipio")

# Colocar el municipio con mayor cantidad en la parte superior
plt.gca().invert_yaxis()

# Escribir la cantidad al final de cada barra
for posicion, valor in enumerate(totales_municipios):
    plt.text(
        valor,
        posicion,
        f" {valor:,}".replace(",", "."),
        va="center"
    )

plt.tight_layout()

# Guardar la gráfica en la carpeta del proyecto
ruta_grafica_municipios = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "grafica_municipios.png"
)

plt.savefig(
    ruta_grafica_municipios,
    dpi=300,
    bbox_inches="tight"
)

plt.close()

print("Grafica de municipios guardada correctamente en:")
print(ruta_grafica_municipios)
# Crear una gráfica con los resultados por año
print("\nCREANDO GRAFICA POR AÑO...")

filas_anuales = resultados_anuales.collect()

anios = [
    fila["ANIO"]
    for fila in filas_anuales
]

totales_anuales = [
    fila["TOTAL_LESIONADOS"]
    for fila in filas_anuales
]

plt.figure(figsize=(12, 6))

plt.plot(
    anios,
    totales_anuales,
    marker="o",
    color="seagreen",
    linewidth=2
)

plt.title(
    "Cantidad de personas lesionadas registradas por año"
)

plt.xlabel("Año")
plt.ylabel("Cantidad de personas lesionadas")

plt.xticks(
    anios,
    rotation=45
)

plt.grid(
    axis="y",
    linestyle="--",
    alpha=0.5
)

plt.tight_layout()

ruta_grafica_anios = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "grafica_anios.png"
)

plt.savefig(
    ruta_grafica_anios,
    dpi=300,
    bbox_inches="tight"
)

plt.close()

print("Grafica por año guardada correctamente en:")
print(ruta_grafica_anios)
# Crear una gráfica con los resultados por mes
print("\nCREANDO GRAFICA POR MES...")

filas_mensuales = resultados_mensuales.collect()

meses = [
    "Enero",
    "Febrero",
    "Marzo",
    "Abril",
    "Mayo",
    "Junio",
    "Julio",
    "Agosto",
    "Septiembre",
    "Octubre",
    "Noviembre",
    "Diciembre"
]

totales_mensuales = [
    fila["TOTAL_LESIONADOS"]
    for fila in filas_mensuales
]

plt.figure(figsize=(12, 6))

plt.bar(
    meses,
    totales_mensuales,
    color="mediumpurple"
)

plt.title(
    "Cantidad acumulada de personas lesionadas por mes"
)

plt.xlabel("Mes")
plt.ylabel("Cantidad de personas lesionadas")

plt.xticks(rotation=45)

# Escribir el valor encima de cada barra
for posicion, valor in enumerate(totales_mensuales):
    plt.text(
        posicion,
        valor,
        f"{valor:,}".replace(",", "."),
        ha="center",
        va="bottom",
        fontsize=8
    )

plt.tight_layout()

ruta_grafica_meses = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "grafica_meses.png"
)

plt.savefig(
    ruta_grafica_meses,
    dpi=300,
    bbox_inches="tight"
)

plt.close()

print("Grafica por mes guardada correctamente en:")
print(ruta_grafica_meses)

spark.stop()

print("\nETAPA DE PREPARACION FINALIZADA CORRECTAMENTE"),