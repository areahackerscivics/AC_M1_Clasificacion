#!/usr/bin/env python
# encoding: utf-8
u"""Módulo de Acceso a datos de los textos descargados de Twitter.

Este módulo contiene los métodos que permiten consultar
campos de la colección que contiene los textos descargados de Twitter
"""
import sys, os
import datetime
from pymongo import MongoClient#Libreria Mongodb
parent_dir=os.getcwd()
path= os.path.dirname(parent_dir)
sys.path.append(path)
from tweetsToText import *

#REAL
from conexionMongo import *

#Conexion a MongoDB
conexion = getConexion()
client = MongoClient(conexion)
tdb = getDB()
db = client[tdb]


def leer():#modificado fecha_creacion
    """Método que permite leer  la colección de textos descargados de Twitter.

    Se  consulta la colección utilizando un find con varias condiciones, como son:
        El idioma debe estar en español
        Los tweets deben ser aquellos que se refieran a @AjuntamentVLC
        La fecha en la que se descargó el tweet sea mayor o igual a la fecha inicial
        y menor que la fecha final.

    Arg:
    fechaini: Fecha inicial
    fechafin: Fecha final

    Resultado:
    idt (list): .
    categoria (list): Categoía que se le han asignados a los textos de forma manual.
    """
    Midt=getMaxIdt()
    coleccion = getCollTweets()
    tweetsdb = db[coleccion]
    idt=[]
    tweet=[]
    fechaTweet=[]
    for text in tweetsdb.find({ "idioma":"es","consulta": "@AjuntamentVLC","idt": { "$gt":str(Midt) } },{"idt":1,"tweet":1,"_id":0,"fecha_creacion":1}):
        idt.append(str(text['idt']))
        tweet.append(str(text['tweet'].encode('utf-8')))
        fechaTweet.append(text['fecha_creacion'])
    if len(idt)>0:
        return idt,tweet,fechaTweet
    else:
        return -1, -1, -1

def getMaxIdt():
    nombre=[]
    coleccion = getCollTweetsClas()
    cursor = db[coleccion]
    Midt=[]
    pipeline=[{"$group":{"_id": 1,"idt": { "$max": "$idt" }}}]
    for text in cursor.aggregate(pipeline):
        Midt.append(str(text['idt']))
    if len(Midt)>0: #validación
        return Midt[0]
    else:
        return -1

def guardar_textoclasificados(corpus,puntaje,clases,idt,fechaTweet):
    coleccion = getCollTweetsClas()
    tweetsdb = db[coleccion]
    today=datetime.datetime.now()
    for i in range(len(corpus)):
        lista= sorted(zip(puntaje[i],  clases), reverse=True)
        categoria=convNumToNom(lista[0][1])
        guardar={
            "categoria":categoria.decode('utf-8'),
            "puntaje":lista[0][0],
            "idt":idt[i], #1
            "texto":corpus[i].decode('utf-8'),
            "fecha":today,
            "fechaTweet":fechaTweet[i]
            };
        try:
            tweetsdb.insert_one(guardar)#MIRAR SI SE PUEDE MEJORAR EL INSERT (INSERT MANY)
        except:
                print "No se pudo guardar"
    print 'Han sido clasificados ',len(corpus),'tweets'
