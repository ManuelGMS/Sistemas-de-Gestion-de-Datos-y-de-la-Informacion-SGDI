import sys
import pyspark.sql.functions as psF
from pyspark.sql import SparkSession

'''
TAREA 1
Crear un DataFrame 'locations' con 3 columnas: identificador de episodio, puntuación IMDB y número de ubicaciones diferentes que aparecen en dicho episodio.
'''
def tarea1(auxEpisodes, scriptLines):

    auxScriptLines = scriptLines.select(scriptLines.episode_id.alias("id"), scriptLines.raw_location_text.alias("location"))
    auxScriptLines = auxScriptLines.dropDuplicates()

    locations = auxEpisodes.join(auxScriptLines, 'id')
    locations = locations.groupBy('id', 'imdb').count().withColumnRenamed("count", "totalLocations")
    # locations.orderBy('id').show(n=600)

    # Cálculo del Coeficiente de Correlación de Pearson dadas dos columnas de un DataFrame.
    # Resultado: -0.00295253 ... --> Existe una pequeñísima correlación negativa.  
    print("CoefPearson entre IMDB y LOCALIZACIONES (Tarea 1):",  locations.stat.corr('imdb', 'totalLocations'))

'''
TAREA 2
Genera un DataFrame characters con 3 columnas: identificador de episodio, puntuación IMDB y número de personajes femeninos diferentes que aparecen. 
Tened en cuenta que hay muchos personajes sin género consignado en nuestros datos de entrada. Estos personajes no se pueden contabilizar como femeninos ni masculinos,
ası́ que los ignoraremos.
'''
def tarea2(auxEpisodes, characters, scriptLines):

    auxCharacters = characters.select('id', 'gender').filter(" gender = 'f' ").drop('gender')

    auxScriptLines = scriptLines.select(scriptLines.episode_id, scriptLines.character_id.alias("id")).dropDuplicates()
    
    characters = auxCharacters.join(auxScriptLines, 'id').withColumnRenamed('id', 'character_id').withColumnRenamed('episode_id', 'id')
    characters = auxEpisodes.join(characters, 'id').groupBy('id','imdb').count().withColumnRenamed('count', 'totalFemeninos')
    # characters.orderBy('id').show(n=600)
    
    # Cálculo del Coeficiente de Correlación de Pearson dadas dos columnas de un DataFrame.
    # Resultado: 0.00611237 ... --> Apenas existe una pequeñísima correlación positiva.  
    print("CoefPearson entre IMDB y FÉMINAS (Tarea 2):",  characters.stat.corr('imdb', 'totalFemeninos'))

'''
TAREA 3
Construye un DataFrame script con 4 columnas: identificador de episodio, puntuación IMDB, número total de palabras que aparecen en los diálogos del episodio y el número total
de diálogos en el episodio. Cuidado: no hay que tener en cuenta las lı́neas del script que no son diálogos.
'''
def tarea3(auxEpisodes, scriptLines):

    auxScriptLines = scriptLines.select('episode_id', 'speaking_line', 'normalized_text').filter(" speaking_line = 'true' ").drop('speaking_line')
    auxScriptLines = auxScriptLines.withColumn("normalized_text", psF.size(psF.split("normalized_text", "\s+")))
    auxScriptLines = auxScriptLines.groupBy("episode_id").agg({"normalized_text": "sum", "*": "count"})
    auxScriptLines = auxScriptLines.withColumnRenamed('episode_id', 'id').withColumnRenamed('sum(normalized_text)', 'totalPalabras').withColumnRenamed('count(1)', 'totalDialogos')
    script = auxEpisodes.join(auxScriptLines, 'id')
    # tarea3.orderBy('id').show(n=600)

    # Cálculo del Coeficiente de Correlación de Pearson dadas dos columnas de un DataFrame.
    # Resultado: 0.23884421 ... --> Existe una pequeña correlación positiva.
    print("CoefPearson entre IMDB y LÍNEAS_DIÁLOGOS (Tarea 3):",  script.stat.corr('imdb', 'totalDialogos'))

    # Cálculo del Coeficiente de Correlación de Pearson dadas dos columnas de un DataFrame.
    # Resultado: 0.27570609 ... --> Existe una pequeña correlación positiva.
    print("CoefPearson entre IMDB y PALABRAS_DIÁLOGOS (Tarea 3):",  script.stat.corr('imdb', 'totalPalabras'))

def main():
    
    # La funcionalidad de Spark es accesible a través de la clase SparkSession.
    spark = SparkSession.builder.getOrCreate()

    # Inferencia del esquema y carga de los datos en el DataFrame.
    episodes = spark.read.csv("simpsons_episodes.csv", header=True, inferSchema=True)
    characters = spark.read.csv("simpsons_characters.csv", header=True, inferSchema=True)
    scriptLines = spark.read.csv("simpsons_script_lines.csv", header=True, inferSchema=True)

    # Este resultado de usa en todas las tareas.
    auxEpisodes = episodes.select(episodes.id, episodes.imdb_rating.alias("imdb"))

    # Resolución de las tareas.
    tarea1(auxEpisodes, scriptLines)
    tarea2(auxEpisodes, characters, scriptLines)
    tarea3(auxEpisodes, scriptLines)

if __name__ == "__main__":
    main()
