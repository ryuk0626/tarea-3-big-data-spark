Procesamiento de datos sobre lesiones en accidentes de tránsito
Autor
Edwin Andrés Tejedor Castiblanco
Ingeniería de Sistemas
Universidad Nacional Abierta y a Distancia - UNAD
Curso: Big Data
Código del curso: 202016911

Descripción
Este proyecto desarrolla un procesamiento batch mediante Apache Spark y Python sobre un conjunto de datos de personas lesionadas en accidentes de tránsito en Colombia.

El programa carga, revisa, transforma y analiza los registros con el propósito de identificar diferencias según el departamento, el municipio, el año y el mes del hecho.

Conjunto de datos
El conjunto de datos fue obtenido del Portal Nacional de Datos Abiertos de Colombia. Contiene las siguientes columnas:

Fecha del hecho.
Código del departamento.
Departamento.
Código del municipio.
Municipio.
Cantidad registrada.
El archivo contiene 644.042 registros.

Procesamiento realizado
El programa realiza las siguientes operaciones:

Carga del archivo CSV mediante un DataFrame de Spark.
Revisión de valores nulos y celdas vacías.
Conversión de la fecha y la cantidad.
Creación de las columnas año y mes.
Revisión de registros aparentemente repetidos.
Cálculo de estadísticas generales.
Agrupación por departamento, municipio, año y mes.
Generación de cuatro gráficas con Matplotlib.
Resultados principales
Total acumulado: 885.829 personas lesionadas registradas.
Departamento con mayor cantidad acumulada: Valle del Cauca.
Municipio con mayor cantidad acumulada: Bogotá D. C.
Año con mayor cantidad registrada: 2016.
Mes con mayor cantidad histórica acumulada: marzo.
Requisitos
Para ejecutar el proyecto es necesario contar con:

Python.
Java.
Apache Spark.
PySpark.
Matplotlib.
Ejecución
El archivo CSV debe guardarse en la misma carpeta del programa con el nombre:

LESIONES_ACCIDENTES_DE_TRÁNSITO_COLOMBIA.csv

Después se ejecuta:

py procesamiento_batch.py

Archivos del repositorio
procesamiento_batch.py: código principal.
grafica_departamentos.png: resultados por departamento.
grafica_municipios.png: resultados por municipio.
grafica_anios.png: resultados por año.
grafica_meses.png: resultados por mes.
Nota
El archivo CSV no se incluye en el repositorio debido a su tamaño. Debe descargarse desde su fuente pública y colocarse en la misma carpeta del código.
