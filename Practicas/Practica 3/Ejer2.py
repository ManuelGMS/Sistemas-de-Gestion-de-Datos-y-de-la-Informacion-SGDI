import sys
import csv
from pyspark.sql import SparkSession

'''
0    1              2                 3                            4            5           6        7
word happiness_rank happiness_average happiness_standard_deviation twitter_rank google_rank nyt_rank lyrics_rank
'''
def wordAndHapinessAverage(line):
    columns = line.split()
    return (columns[0], columns[2])

'''
0  1          2      3        4               5             6            7           8                  9                 10           11              12       
id episode_id number raw_text timestamp_in_ms speaking_line character_id location_id raw_character_text raw_location_text spoken_words normalized_text word_count
'''
def simpsonColumns(line):
    columns = list(csv.reader([line]))[0]
    return None if (len(columns) < 12) else (columns[1], columns[5], columns[11])
    
def main():
    
    # La funcionalidad de Spark es accesible a través de la clase SparkSession.
    spark = SparkSession.builder.getOrCreate()
    
    # Contendrá métodos para indicar el contexto en el que se trabaja.
    sc = spark.sparkContext

    # RDD
    happiness = sc.textFile("happiness.txt")
    happiness = happiness.map(wordAndHapinessAverage) # (word, happiness_average)
  
    # RDD
    simpsonsScriptLines = sc.textFile("simpsons_script_lines.csv")
    simpsonsScriptLines = simpsonsScriptLines.map(simpsonColumns) # (episode_id, speaking_line, normalized_text)
    simpsonsScriptLines = simpsonsScriptLines.filter(lambda line: line != None and line[0] != 'episode_id' and line[1] != 'false') # Filtro para cabecera y no diálogos.
    simpsonsScriptLines = simpsonsScriptLines.map(lambda line: (line[0], line[2])) # (episodio, fragmento de diálogo)
    simpsonsScriptLines = simpsonsScriptLines.flatMap(lambda episodeWords: [ (word, episodeWords[0]) for word in episodeWords[1].split(" ")]) # (word, episode)

    # Resolución
    fusion = simpsonsScriptLines.join(happiness) # (word, (episode, value))
    fusion = fusion.map(lambda wordAndEpisodeValue: ((wordAndEpisodeValue[0], wordAndEpisodeValue[1][0], wordAndEpisodeValue[1][1]), 1)) # ((word, episode, value), 1)
    fusion = fusion.reduceByKey(lambda appears1, appears2: appears1 + appears2) # ((word, episode, value), appears)
    fusion = fusion.map(lambda wordEpisodeValueAndAppears: (wordEpisodeValueAndAppears[0][1], float(wordEpisodeValueAndAppears[0][2]) * float(wordEpisodeValueAndAppears[1]))) # (episode, totalOfWord)
    fusion = fusion.reduceByKey(lambda value1, value2: round(value1 + value2, 2)) # (episode, totalOfWords)
    fusion = fusion.sortBy(keyfunc=lambda episodeTotal: episodeTotal[1], ascending=False) # Order by total happiness.

    # Resultado
    for episodeTotal in fusion.collect():
        print(episodeTotal[0], episodeTotal[1])

if __name__ == "__main__":
    main()
