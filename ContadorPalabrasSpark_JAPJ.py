### CONTADOR DE PALABRAS USANDO SPARK/HADOOP - Análisis de Aplicaciones de Big Data - JAPJ


# Contador de Palabras con Apache Spark - Análisis de Aplicaciones de Big Data - JAPJ
# Importar las librerías necesarias de PySpark

from pyspark.sql import SparkSession
from pyspark.sql.functions import *



# 1. Inicializar sesión de Spark
# Crear sesión de Spark con configuración optimizada

spark = SparkSession.builder \
    .appName("WordCounterAIApplications") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()



# 2. Carga y Preparación de Datos
# Configurar nivel de logging para mejor visualización

spark.sparkContext.setLogLevel("WARN")


print("\n\n\n")
print("=" * 68)
print("CONTADOR DE PALABRAS - APLICACIONES DE BIG DATA - G-7")
print("=" * 68)


# 3. Cargar el archivo de texto como RDD
# RDD --->> Resilient Distributed Dataset


text_rdd = spark.sparkContext.textFile("Annex_1_Aplication.txt")



# 4. Mostrar información básica del dataset
print(f"\n\n INFORMACIÓN DEL DATASET - ANEXO 1:")
print(f"   • Número total de líneas: {text_rdd.count()}")
print(f"   • Primeras 12 líneas: {text_rdd.take(12)}")




# 5. Transformaciones para contar TRES palabras guia Actividad 4
print(f"\n\n ➤ CONTEO DE PALABRAS ACTIVIDAD 3:")



# Contar 'Recognition' 

# filter >>> Selecciona líneas que contengan 'Recognition'
# flatMap >>> Divide líneas en palabras individuales
# filter >>> Filtra solo la palabra exacta ' Recognition' con el Espacio inicial " R..."
# count >>> Cuenta las Ocurrencias


recognition_count = text_rdd.filter(lambda line: 'Recognition' in line) \
                           .flatMap(lambda line: line.split()) \
                           .filter(lambda word: word == 'Recognition') \
                           .count()



# Contar 'Vision' 
vision_count = text_rdd.filter(lambda line: 'Vision' in line) \
                      .flatMap(lambda line: line.split()) \
                      .filter(lambda word: word == 'Vision') \
                      .count()


# Contar ' Recognition' 
recognition_space_count = text_rdd.filter(lambda line: 'Recognition' in line) \
                                 .flatMap(lambda line: line.split()) \
                                 .filter(lambda word: word == ' Recognition') \
                                 .count()





# 6. Resultados 3 palabras solicitadas
print(f"   •➤ 'Recognition': {recognition_count} Ocurrencias")
print(f"   •➤ 'Vision': {vision_count} Ocurrencias")
print(f"   •➤ ' Recognition' con el Espacio inicial: {recognition_space_count} Ocurrencias")



# 7. Análisis completo de todas las palabras >>> más allá de lo propuesto por la guia
print(f"\n\n\n ﴿﴿    ANÁLISIS COMPLETO DE TODAS LAS PALABRAS - ANEXX 1  ﴾﴾")


# Conteo completo de todas las palabras:

# flatMap >>> Tokenización del texto  
# map >>> Crear pares, parejas (palabra, 1)
# reduceByKey >>> Sumar ocurrencias por palabra
# sortBy >>> Ordenar por frecuencia descendente


todas_palabras_count = text_rdd.flatMap(lambda line: line.split()) \
                         .map(lambda word: (word, 1)) \
                         .reduceByKey(lambda a, b: a + b) \
                         .sortBy(lambda x: x[1], ascending=False)



# 8. Mostrar top 10 palabras más frecuentes
print("\n TOP 10 PALABRAS MÁS FRECUENTES - ANEXX 1:")
top_words = todas_palabras_count.take(10)
for i, (word, count) in enumerate(top_words, 1):
    print(f"   {i:2d}. {word:15} → {count:3d} ocurrencias")


# 9. Análisis estadístico adicional
total_palabras = todas_palabras_count.map(lambda x: x[1]).sum()
palabras_unicas = todas_palabras_count.count()



print(f"\n ESTADÍSTICAS GENERALES - ANEXX 1:")
print(f"   • Total de palabras: {total_palabras}")
print(f"   • Palabras únicas: {palabras_unicas}")
print(f"   • Palabra más frecuente: '{top_words[0][0]}' ({top_words[0][1]} veces)")


# 10. Convertir a DataFrame para mejor visualización
df_palabras = todas_palabras_count.toDF(["word", "count"])



# 11. Mostrar resultados en formato tabla
print(f"\n RESUMEN EN FORMATO TABULAR  - ANEXX 1:")
df_palabras.show(25, truncate=False)



# 12. Generar HTML con resultados 

# El archivo HTML incluye:
# Resultados específicos de las palabras solicitadas
#Tabla ordenada del top 10 palabras
#Estadísticas generales del análisis
#Diseño responsive con CSS integrado


html_output = f"""
<!DOCTYPE html>
<html>
<head>
    <title>CONTADOR DE PALABRAS USANDO SPARK/HADOOP - Análisis de Aplicaciones de Big Data - JAPJ</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 40px; }}
        .header {{ background: #2c3e50; color: white; padding: 20px; border-radius: 10px; }}
        .results {{ margin: 20px 0; }}
        .word-result {{ background: #ecf0f1; padding: 15px; margin: 10px 0; border-radius: 5px; }}
        .table {{ width: 100%; border-collapse: collapse; }}
        .table th, .table td {{ padding: 12px; text-align: left; border-bottom: 1px solid #ddd; }}
        .table th {{ background-color: #34495e; color: white; }}
    </style>
</head>
<body>
    <div class="header">
        <h1> Contador de Palabras con Apache Spark - Análisis de Aplicaciones de Big Data - JAPJ</h1>
        <p>Análisis de frecuencia de palabras en aplicaciones de IA</p>
    </div>
    
    <div class="results">
        <h2> Resultados Específicos - JAPJ</h2>
        <div class="word-result">
            <strong>'Recognition':</strong> {recognition_count} Ocurrencias
        </div>
        <div class="word-result">
            <strong>'Vision':</strong> {vision_count} Ocurrencias
        </div>
        <div class="word-result">
            <strong>' Recognition' con Espacio inicial:</strong> {recognition_space_count} Ocurrencias
        </div>
    </div>
    
    <div class="results">
        <h2> Top 10 Palabras Más Frecuentes - JAPJ</h2>
        <table class="table">
            <tr><th>Posición</th><th>Palabra</th><th>Frecuencia</th></tr>
"""



# Añadir filas de la tabla
for i, (word, count) in enumerate(top_words, 1):
    html_output += f'            <tr><td>{i}</td><td>{word}</td><td>{count}</td></tr>\n'

html_output += f"""
        </table>
    </div>
    
    <div class="results">
        <h2> Estadísticas Generales</h2>
        <ul>
            <li><strong>Total de Palabras:</strong> {total_palabras}</li>
            <li><strong>Palabras Únicas:</strong> {palabras_unicas}</li>
            <li><strong>Palabra más Frecuente:</strong> '{top_words[0][0]}' ({top_words[0][1]} Ocurrencias)</li>
        </ul>
    </div>
</body>
</html>
"""




# 13. Guardar HTML

with open("Resultados_Contador_Palabras_JAPJ.html", "w", encoding="utf-8") as f:
    f.write(html_output)

print(f"\n ✅ ARCHIVO HTML GENERADO: 'Resultados_Contador_Palabras_JAPJ.html'")





# 14. Cerrar sesión de Spark
spark.stop()

print(f"\n✔️ ANÁLISIS TAREA 3 COMPLETADO EXITOSAMENTE")
print("=" * 68)
