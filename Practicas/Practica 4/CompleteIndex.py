import os
import string
from collections import OrderedDict

# Dada una linea de texto, devuelve una lista de palabras no vacias 
# convirtiendo a minusculas y eliminando signos de puntuacion por los extremos
# Ejemplo:
#   > extrae_palabras("Hi! What is your name? John.")
#   ['hi', 'what', 'is', 'your', 'name', 'john']
def extrae_palabras(linea):
    return list(filter(lambda x: len(x) > 0, map(lambda x: x.lower().strip(string.punctuation), linea.split())))

class CompleteIndex(object):

    # ********************************************************************************************************************************************************************************

    def __init__(self, directorio, compresion=None):
        
        # Variables propias de cada instancia de esta clase.
        self.idFichero = 0 # Para asignar identificadores univocos a los ficheros, también para contar el total de documentos en la colección (N).
        self.rutaAlFichero = dict() # dict[documento] --> Ruta asociada al documento.
        self.indiceCompleto = dict() # dict[termino] --> [ (documento,[pos1, ... , posN]) , ... , (documento,[pos1, ... , posN]) ]
        self.numeroDePalabraEnFichero = dict() # dict[documento] --> Nº De palabra en el documento asociado.

        # Recorremos todos los directorios y los ficheros de estos.
        for ruta, _, ficheros in os.walk(directorio):
            # Para cada fichero encontrado.
            for nombreFichero in ficheros:
                # A cada fichero se le otorga un ID.
                self.idFichero += 1
                # Guardamos la ruta en la que hemos encontrado el fichero.
                self.rutaAlFichero[self.idFichero] = ruta + "/" + nombreFichero
                # Inicializamos en contador de palabras por cada documento.
                self.numeroDePalabraEnFichero[self.idFichero] = 0
                # Abrimos cada fichero encontrado en modo lectura y con una codificación que permita leerlo.
                with open(self.rutaAlFichero[self.idFichero], 'r', encoding="ISO-8859-1") as fichero:
                    # Recorremos cada línea del fichero.
                    for linea in fichero:
                        # Para cada palabra extraida de la línea leída.
                        for palabra in extrae_palabras(linea):
                            # Incremento el contador de palabras/posiciones en el fichero.
                            self.numeroDePalabraEnFichero[self.idFichero] += 1
                            # Si el término no está en el índice se inicializa un diccionario ordenado, este recuerda el orden de inserción de los items.
                            if palabra not in self.indiceCompleto:
                                self.indiceCompleto[palabra] = OrderedDict()
                            # Cuando se detecta por primera vez una palabra en un fichero concreto, se crea la lista de las posiciones en la que dicha palabra aparece en él.
                            if self.idFichero not in self.indiceCompleto[palabra]:
                                self.indiceCompleto[palabra][self.idFichero] = []
                            # Dada una palabra, se añade a lista del fichero actual la posición en la que aparece el término.
                            self.indiceCompleto[palabra][self.idFichero].append(self.numeroDePalabraEnFichero[self.idFichero])


        # Recorremos cada término y ficheros asociados a este.
        for palabra, ficheros in self.indiceCompleto.items():
            # Aquí crearemos la lista [ [fichero1, [pos1, pos2, ... , posN]] , ... , [ficheroN, [pos1, pos2, ... , posN]] ] asociada a un término concreto.
            listaAuxiliar = []
            # Para cada fichero asociado al término, introducimos en la lista elementos tipo [fichero, [pos1, pos2, ... , posN]
            for idFichero, listaApariciones in ficheros.items():
                listaAuxiliar.append([idFichero, listaApariciones])
            # Asignamos la lista completa a la entrada de la palabra actual.
            self.indiceCompleto[palabra] = listaAuxiliar

    # ********************************************************************************************************************************************************************************

    def __noTodasVacias(self, listaDeListas):
        # Se comprueba si p1 ... PN están vacías.
        return all(len(lista) > 0 for lista in listaDeListas)

    def __mismoDocId(self, listaDeListas):
        # Creo una lista que contiene el primer documento en el que aparece cada término.
        primerosDocId = [lista[0][0] for lista in listaDeListas]
        # Compruebo que el identificador de todos sea igual al primero para garantizar que sean iguales, es decir, que para todo término estamos en el mismo documento.
        return all(primerosDocId[0] == docId for docId in primerosDocId)
    
    def __avancarDocIdMenores(self, listaDeListas):
        # Creo una lista que contiene el primer documento en el que aparece cada término.
        primerosDocId = [lista[0][0] for lista in listaDeListas]
        # Obtengo el menor identificador de fichero en la lista.
        menorDocId = min(primerosDocId)
        # Devuelvo el resultado de suprimir en cada sub-lista el primer elemento cuyo identificador es el del documento con menor identificador.
        return [ lista[1:len(lista)] if lista[0][0] == menorDocId else lista for lista in listaDeListas ]

    def __buscarTerminosConsecutivos(self, listaDeListas):

        # Para salir del bucle.
        salir = False

        # Para saber la frase se encuentra dentro del mismo fichero.
        estaElPatron = False

        while not salir:

            # Obtenemos por cada sub-lista el primer elemento del array de posiciones de un término concreto.
            primerasApariciones = [lista[0][1][0] for lista in listaDeListas if len(lista[0][1]) > 0]

            # Si el número de posiciones (elementos) en la lista, es menor que el número de sub-listas, entonces la lista de posiciones de algún documento se ha vaciado.
            if len(primerasApariciones) == len(listaDeListas):

                # Se comprueba que los términos aparezcan de forma consecutiva en el texto.
                i = 0
                consecutivos = True
                while i < (len(primerasApariciones) - 1) and consecutivos:
                    if primerasApariciones[i] + 1 != primerasApariciones[i+1]:
                        consecutivos = False
                    i = i + 1

                if consecutivos:
                    # Si los términos aparecen de forma consecutiva, entonces el documento es una solución.
                    salir = True
                    estaElPatron = True
                else:
                    # Obtenemos la menor posición de un documento en la que aparece un término dentro de este.
                    menorAparicion = min(primerasApariciones)
                    # Recorro todas las sub-listas.
                    for lista in listaDeListas:
                        # Encontramos la sub-lista cuyo primer elemento (documento) tiene como primer elemento de su lista de posiciones, la de la menor posición. 
                        if lista[0][1][0] == menorAparicion:
                            # Eliminamos la menor posición de la lista.
                            lista[0][1] = lista[0][1][1:len(lista[0][1])]
                
            else:
                
                salir = True # Alguna lista de apaiciones de un término en un documento se ha vaciado.

        return estaElPatron

    def __intersect_phrase(self, listaDeListas):

        # Array que contendrá los documentos encontrados.      
        answer = []

        while self.__noTodasVacias(listaDeListas):
            
            if self.__mismoDocId(listaDeListas):
                if self.__buscarTerminosConsecutivos(listaDeListas):
                    # Incorporamos a la solucion el documento en el que se cumple la aparición de la frase.
                    answer.append(listaDeListas[0][0][0])
            
            listaDeListas = self.__avancarDocIdMenores(listaDeListas)

        return answer

    def consulta_frase(self, frase):
        
        # p1 = [ [doc1, [pos1 , ... , posN]], ... , [docN, [pos1 , ... , posN]] ]
        # listaDeListas --> [ p1, p2, p3 ]
        listaDeListas = []

        # Se comprobará que todas las palabras están en el índice, de lo contrario, no podrá encontrarse la frase.
        i = 0
        palabraImposible = False
        palabras = extrae_palabras(frase)
        while i < len(palabras) and not palabraImposible:
            if palabras[i] not in self.indiceCompleto:
                palabraImposible = True
            else:
                listaDeListas.append(self.indiceCompleto[palabras[i]])
            i = i + 1

        # Lista que contendrá los documentos con la frase introducida.
        encontrados = []

        # Vemos que todas las palabras estén en el índice.
        if not palabraImposible:
            # Buscamos en los documentos la frase introducida.
            encontrados = self.__intersect_phrase(listaDeListas)
        
        # Devolvemos la lista con los documentos encontrados.
        return [ self.rutaAlFichero[documento] for documento in encontrados ]

    # ********************************************************************************************************************************************************************************

if __name__ == '__main__':

    i = CompleteIndex("20news-18828")

    # print(i.consulta_frase('either terrestrial or alien'))
    # print(i.consulta_frase('is more complicated'))
    # print(i.consulta_frase("a general introduction"))
    # print(i.consulta_frase("hola"))
    # print(i.consulta_frase("Esta prueba no devuelve resultados"))
