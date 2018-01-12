#!/usr/bin/env python
# encoding: utf-8
def convNumToNom(num):
    dicNumToNom={
    0:'Ciencia y tecnología',
    1:'Comercio',
    2:'Cultura y ocio',
    3:'Demografía',
    4:'Deporte',
    5:'Economía',
    6:'Educación',
    7:'Empleo',
    8:'Energía',
    9:'Hacienda',
    10:'Industria',
    11:'Legislación y justicia',
    12:'Medio ambiente',
    13:'Medio Rural',
    14:'Salud',
    15:'Sector público',
    16:'Seguridad',
    17:'Sociedad y bienestar',
    18:'Transporte',
    19:'Turismo',
    20:'Urbanismo e infraestructuras',
    21:'Vivienda'
    }

    categoria=dicNumToNom[num]
    return categoria
