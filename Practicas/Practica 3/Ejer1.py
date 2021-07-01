import re
import json
from mrjob.job import MRJob

class Ejer1(MRJob):

    def mapper(self, key, line):
        wordCounter = dict()
        documento = json.loads(line)
        fileName = documento["filename"]
        palabras = re.sub('[^a-z ]', '', documento["content"].lower()).split(" ")

        for palabra in palabras:
            if palabra != "":
                if palabra not in wordCounter:
                    wordCounter[palabra] = 1
                else:
                    wordCounter[palabra] += 1

        for palabra in wordCounter.keys():
            yield palabra, (fileName, wordCounter[palabra]) 

    def combiner(self, key, values):
        
        maxFilenameCounter = max(values, key = lambda pair : pair[1])

        yield key, (maxFilenameCounter[0], maxFilenameCounter[0])

    def reducer(self, key, values):

        maxFilenameCounter = max(values, key = lambda pair : pair[1])

        yield key, "<" + maxFilenameCounter[0] + ", " + str(maxFilenameCounter[1]) + ">"

if __name__ == '__main__':
    Ejer1.run()
