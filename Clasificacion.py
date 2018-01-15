#!/usr/bin/env python
# encoding: utf-8

import sys, os, io
import json
import cPickle as pickle
from normalizacion import *
from tweetsToText import convNumToNom
from clasificacionDAO import *
from datetime import datetime



# 2 Vectorizador
def transformar(textos, nombre):
    textosNormalizados = normalizar_corpus(textos)
    vectorizador = leer_Pickle('vectorizer_'+ nombre +'.pickle')
    data = vectorizador.transform(textosNormalizados)
    data = data.toarray()
    return data
#  fin Vectorizador

# 3 procesopickle
def leer_Pickle(parametro):
    datafile = os.path.abspath(parametro)
    #datafile=os.path.join("AC_M1_Clasificacion",parametro) #ruta crontab linux
    fichero = file(datafile)
    variable = pickle.load(fichero)
    return variable
#fin procesopickle


def generar_clasificacion():
    print str(datetime.now())," - Inicia el proceso de clasificación...".decode('utf-8')

    #1--------------------cargar el corpus-------------------
    idt,corpus,fechaTweet=leer()

    if idt!=-1:#validación del método leer, por si no hay registros
        #CONSULTAR EN BASE DE DATOS EL NOMBRE DEL CLASIFICADOR (DAO)
        nombre="MaryPr"
        if nombre!=-1:#validación de vacio
            tfidf=transformar(corpus,nombre)
            SGDtfidf=leer_Pickle(nombre +'.pickle')
            #print 'Se ha usado el vectorizador....'
            clases=SGDtfidf.classes_ #generando las clases
            puntaje=SGDtfidf.decision_function(tfidf) #generando puntajes
            #print 'Se ha usado el clasificador....'
            #4--------------------Guardar  en db Clasificador-------------------

            #escribirClasificados(corpus,puntaje,clases,idt)
            guardar_textoclasificados(corpus,puntaje,clases,idt,fechaTweet)
    else:
        print str(datetime.now())," - No hay tweets para clasificar"
#Fin BLL
generar_clasificacion()
