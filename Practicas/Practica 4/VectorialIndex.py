import os
import string
from math import log2, sqrt
from itertools import groupby

# Dada una linea de texto, devuelve una lista de palabras no vacias 
# convirtiendo a minusculas y eliminando signos de puntuacion por los extremos
# Ejemplo:
#   > extrae_palabras("Hi! What is your name? John.")
#   ['hi', 'what', 'is', 'your', 'name', 'john']
def extrae_palabras(linea): # Se añade list
  return list(filter(lambda x: len(x) > 0, map(lambda x: x.lower().strip(string.punctuation), linea.split())))

class VectorialIndex(object):

    # ********************************************************************************************************************************************************************************

    def __init__(self, directorio):


        # Variables propias de cada instancia de esta clase.
        self.idFichero = 0 # Para asignar identificadores univocos a los ficheros, también para contar el total de documentos en la colección (N).
        self.rutaAlFichero = dict() # dict[documento] --> Ruta asociada al documento.
        self.indiceVectorial = dict() # dict[termino] --> [ [doc1, peso] , ... , [docN, peso] ]
        self.pesoTotalDocumento = dict() # Norma |dj| del denominador de la formula R(dj,q) sin la raíz cuadrada.


        # Recorremos todos los ficheros de todos las carpetas que están bajo la que se pasa como parámetro.
        for ruta, _, ficheros in os.walk(directorio):
            # Recorremos todos los ficheros del directorio actual.
            for nombreFichero in ficheros:
                # A cada fichero se le otorga un ID.
                self.idFichero += 1
                # Cada ID tiene asociado la ruta absoluta de donde se encuentra el fichero dentro de la carpeta a analizar.
                self.rutaAlFichero[self.idFichero] = ruta + "/" + nombreFichero
                # Inicialización de la norma |dj| para todo documento de la colección.
                self.pesoTotalDocumento[self.idFichero] = 0
                # Abrimos el fichero actual en modo lectura e indicando su codificación.
                with open(self.rutaAlFichero[self.idFichero], 'r', encoding="ISO-8859-1") as fichero:
                    # Recorremos línea a línea el fichero actual.
                    for linea in fichero:
                        # Recorremos cada palabra de la línea actual.
                        for palabra in extrae_palabras(linea):
                            # Almacenamos para cada termino/palabra todos los ficheros donde esta aparece: palabra --> [fichero fichero fichero fichero fichero fichero]
                            if palabra not in self.indiceVectorial:
                                self.indiceVectorial[palabra] = [self.idFichero]
                            else:
                                self.indiceVectorial[palabra].append(self.idFichero)
        

        # Recorremos cada término y lista asociada a este.
        for palabra, lista in self.indiceVectorial.items():

            # Aquí crearé la verdadera lista asociada a un término.            
            listaAuxiliar = []
            
            # Al cababar este bucle listaAuxiliar es igual que a lista asociada a un término en un índice invertido [ [doc1, frecuencia] , ... , [docN, frecuencia] ]
            for fichero, grupo in groupby(lista):
                listaAuxiliar.append([fichero, len(list(grupo))])

            # Número de ficheros en los que aparece el término.
            ni = len(listaAuxiliar)
            # Número total de ficheros que hay en nuestra colección.
            N = self.idFichero

            # Recorremos todas las parejas [doc, frecuencia] de la lista.
            for ficheroFrecuencia in listaAuxiliar:
                # Asignamos el peso con TF-IDF (Term Frequency - Inverse Document Frequency).
                ficheroFrecuencia[1] = (1 + log2(ficheroFrecuencia[1])) * log2(N/ni)
                # Calculamos el sumatorio del denominador pero sin la raíz cuadrada.
                self.pesoTotalDocumento[ficheroFrecuencia[0]] += ficheroFrecuencia[1] * ficheroFrecuencia[1]
            
            # Machacamos la entrada del término con la lista de la forma [ [doc1, peso] , ... , [docN, peso] ] que es la propia de un índice vectorial.
            self.indiceVectorial[palabra] = listaAuxiliar

    # ********************************************************************************************************************************************************************************

    def __calcularPesosTerminosConsulta(self, consulta):

        # Peso acumulado de los terminos que aparecen la consulta para cada documento que contenga alguna de estos términos.
        pesoTerminosConsulta = dict()

        # Para cada término en la consulta.
        for termino in extrae_palabras(consulta):
            # Obtengo todos los pares (documento, peso)
            for documento, peso in self.indiceVectorial[termino]:
                if documento not in pesoTerminosConsulta:
                    # Inicializo el peso asociado a la consulta para el documento.
                    pesoTerminosConsulta[documento] = peso
                else:
                    # Acumulo el peso asociado a la consulta para el documento.
                    pesoTerminosConsulta[documento] += peso 

        # Devuelvo el total de pesos acumulado para cada documento que contiene una palabra de la consulta.
        return pesoTerminosConsulta

    def consulta_vectorial(self, consulta, n=3):

        # Se comprobará que todas las palabras están en el índice, de lo contrario, no podrá ejecutarse la consulta.
        i = 0
        palabraImposible = False
        palabras = extrae_palabras(consulta)
        while i < len(palabras) and not palabraImposible:
            if palabras[i] not in self.indiceVectorial:
                palabraImposible = True
            i = i + 1
        
        # Creamos una lista de parejas [ (RutaAbsolutaAlFichero1, total1) , ... , (RutaAbsolutaAlFicheroN, totalN) ]
        ranking = []

        # Si todas las palabras están en el índice.
        if not palabraImposible:

            # Calculamos el numerador de la fórmula R(dj,q)
            pesosTerminosConsulta = self.__calcularPesosTerminosConsulta(consulta)

            # Calculamos para todos los documentos que acumularon pesos asociados a las palabras de las consultas el total.    
            for documento, _ in pesosTerminosConsulta.items():
                ranking.append((self.rutaAlFichero[documento], pesosTerminosConsulta[documento] / sqrt(self.pesoTotalDocumento[documento])))    

        # Ordenamos la lista anterior por el campo de los totales y devolvemos la lista ordenada.
        return sorted(ranking, key=lambda pair: pair[1], reverse=True)[ 0 : n if n > 0 else 3 ]

    # ********************************************************************************************************************************************************************************

    def __docId(self, lst):
        '''
        Si la lista recivida es "answer" más allá de la primera iteración del bucle de "intersecMultiple", lista es una lista de enteros, y hay que devolver el primer elemento.
        Si la lista recivida no es "answer" (o lo es en la primera iteración del bucle de "intersecMultiple"), la lista es [ (doc1, peso) , ... , (docN, peso) ] y por tanto hemos
        de devolver, del primer par, el identificador del documento.
        '''
        return lst[0] if isinstance(lst[0], int) else lst[0][0]

    def __intersec2(self, lista1, lista2):
        
        # Lista que contendrá los documentos en los que se ha encontrado la intersección de dos términos.
        answer = []
        
        # Hasta que nos quedemos sin documentos en las listas de los términos a intersecar.
        while lista1 != [] and lista2 != []:
            # Si ambos términos se encuentran dentro de un mismo documento.
            if self.__docId(lista1) == self.__docId(lista2):
                # Se añade el documento como solución.
                answer.append(self.__docId(lista1))
                # Cada lista elimina el primer documento que contienen.
                lista1 = lista1[1:len(lista1)]
                lista2 = lista2[1:len(lista2)]
            elif self.__docId(lista1) < self.__docId(lista2):
                # Los documentos no coinciden, se elimina el documento con menor identificador de la lista, pasando así al siguiente para intentar que coincidan.
                lista1 = lista1[1:len(lista1)]
            else:
                # Los documentos no coinciden, se elimina el documento con menor identificador de la lista, pasando así al siguiente para intentar que coincidan.
                lista2 = lista2[1:len(lista2)]
        
        # Devolvemos la lista con los documentos en los que se ha encontrado la intersección de dos términos.
        return answer

    def __intersecMultiple(self, listaDeListas):
        
        # Ordenamos las listas de menor a mayor tamaño.
        terms = sorted(listaDeListas, key=len)
        # Obtenemos la primera sub-lista "p".
        answer = terms[0:1][0]
        # Obtenemos las otras sub-listas "p".
        terms = terms[1:len(terms)] 
        
        '''
        Si nos quedamos sin "terms" y "answer" está lleno, entonces tenemos en el propio "answer" una lista con los documentos que componen la solución.
        Si en cualquier iteración answer está vacío, entonces no se han podido intersecar todos los términos dentro de un mismo documento, por lo que ya no habría solución.
        '''
        while terms != [] and answer != []:
            # Obtenemos la siguiente primera sub-lista "p".
            e = terms[0:1][0]
            # Buscamos en las listas que los dos terminos se encuentren en un mismo documento.
            answer = self.__intersec2(answer, e)
            # Eliminamos la primera sub-lista "p".
            terms = terms[1:len(terms)]
        
        # Deolvemos una lista con todos los documentos en los que se han encontrado todos los términos introducidos.
        return answer

    def consulta_conjuncion(self, consulta):
        
        # p1 = [ [doc1, peso], ... , [docN, peso] ]
        # listaDeListas --> [ p1, p2, p3 ]
        listaDeListas = []

        # Se comprobará que todas las palabras están en el índice, de lo contrario, no podrá encontrarse la conjunción.
        i = 0
        palabraImposible = False
        palabras = extrae_palabras(consulta)
        while i < len(palabras) and not palabraImposible:
            if palabras[i] not in self.indiceVectorial:
                palabraImposible = True
            else:
                listaDeListas.append(self.indiceVectorial[palabras[i]])
            i = i + 1

        # Lista que contendrá los documentos con las palabras introducidas.
        encontrados = []

        # Vemos que todas las palabras estén en el índice.
        if not palabraImposible:
            
            # Si solo se introduce una única palabra, haremos la conjunción con ella misma (palabra AND palabra).
            if len(palabras) == 1:
                listaDeListas.append(self.indiceVectorial[palabras[0]])

            # Buscamos todos los documentos en los que se encuentren las palabras introducidas.
            encontrados = self.__intersecMultiple(listaDeListas)

        # Devolvemos la lista con los documentos encontrados.
        return [ self.rutaAlFichero[documento] for documento in encontrados ]

    # ********************************************************************************************************************************************************************************

if __name__ == '__main__':

    # **** INDICE VECTORIAL ****
    i = VectorialIndex("20news-18828")

    # **** CONJUNCION BOOLEANA ****
    # print(i.consulta_conjuncion("DES Diffie-Hellman"))
    # print(i.consulta_conjuncion("got back on the street"))
    # print(i.consulta_conjuncion("no larger tank available"))
    # print(i.consulta_conjuncion("hola"))
    # print(i.consulta_conjuncion("Esta prueba no devuelve resultados"))


    # **** CONSULTA VECTORIAL ****
    # print(i.consulta_vectorial("DES Diffie-Hellman", n=5))
    # print(i.consulta_vectorial("got back on the street", 30))
    # print(i.consulta_vectorial("no larger tank available"))
    # print(i.consulta_vectorial("hola"))
    # print(i.consulta_vectorial("Esta prueba no devuelve resultados"))
