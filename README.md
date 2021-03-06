# Sistema de clasificación


## Descripción

Este repositorio contiene el módulo principal para clasificar textos en el marco del proyecto "Sistema automático de clasificación de mensajes intercambiados entre la ciudadanía y el Ayuntamiento de València". A partir de los canales de comunicación del Ayuntamiento de València se ha generado un formato que cualquier consistorio puede adaptar a sus necesidades.

El trabajo realizado se concreta en forma de código fuente  que  está diseñada para clasificar textos, inicialmente tweets.


## Guía de uso

##### Lenguaje de programación
Este módulo fue desearrollado con **Python 2.7.11**

##### Dependencias

* [Pymongo v3.4.0](https://api.mongodb.com/python/current/ "Pymongo 3.4.0")
* [nltk v3.2.4](http://www.nltk.org/)
* [scikit-learn v0.18.2](http://scikit-learn.org/stable/index.html)
 * [NumPy v1.13.1](http://www.numpy.org/)
 * [SciPy v0.19.1](https://www.scipy.org/)

**Nota**: El modulo fue desarrollado usando las librerías que se mencionaron anteriormente, por lo que se recomienda  que para un adecuado funcionamiento se usen  las versiones establecidas. Si durante la instalación de Scipy da un error de memoria prueba
```
pip install scipy==0.19.0 --no-cache-dir
```

##### Base de datos

Como sistema de almacenamiento se usa MongoDB que es una base de datos NoSQL que guarda los datos en documentos almacenados en BSON. Para este módulo se usa una colección que tiene la siguiente estructura:

```json
TWEET
{
    "_id" : ObjectId("58adb1bebc54f400e09531ea"),
    "username" : "pepito",
    "userSeguidores" : 13532,
    "consulta" : "@vivons",
    "idioma" : "es",
    "idt" : "123456789",
    "userid" : 1234,
    "fecha_creacion" : ISODate("2017-02-22T15:40:19.000Z"),
    "dispositivo" : "TweetDeck",
    "tweet" : "Que viva la vida",
    "fechaTweet" : ISODate("2017-02-28T00:00:00.000Z")
}

```
```json
TWCLASIFICADO
{
    "_id" : ObjectId("58de2570bc54f43b544428e0"),
    "categoria" : "Seguridad",
    "idt" : "123456789",
    "texto" : "este texto habla sobre seguridad",
    "puntaje" : 0.508395516543446,
    "fecha" : ISODate("2017-03-31T11:46:24.669Z"),
    "fechaTweet" : ISODate("2017-03-22T10:12:51.000Z")
}

```

La colección **TWEET** se crea en tiempo de ejecución la primera vez que se ejecuta el código _DescargaTweet.py_  que se encuentra dentro del módulo  <a href="https://github.com/areahackerscivics/DescargaTweet" target="_blank_"> DescargaTweet </a>.

La colección **TWCLASIFICADO** contiene los textos clasificados automáticamente.

En el archivo **ConexionMongoPublico.py**, se indican los nombres de las colecciones y la base de datos con la que se trabajó, si desea poner otro nombre a la base de datos o  de alguna de las colecciones, es necesario que actualice el archivo. Finalmente debe cambiar  el nombre a  **ConexionMongo.py** .

El Àrea se abstiene de publicar los datos almacenados, debido a la Ley Orgánica de Protección de Datos.


## Equipo
- Autores principales:  

  - **<a href="https://www.linkedin.com/in/marylin-mattos-a0a59b22/" target="_blank"> Marylin Mattos Barros</a>**, estudiante de Máster Oficial Universitario en Gestión de la Información
  


- Director del proyecto:
  - [Diego Álvarez](https://about.me/diegoalsan) | @diegoalsan


## Contexto del proyecto

El trabajo que contiene este repositorio se ha desarrollado en el [**Àrea Hackers cívics**](http://civichackers.cc). Un espacio de trabajo colaborativo formado por [hackers cívics](http://civichackers.webs.upv.es/conocenos/que-es-una-hacker-civicoa/) que buscamos y creamos soluciones a problemas que impiden que los ciudadanos y ciudadanas podamos influir en los asuntos que nos afectan y, así, construir una sociedad más justa. En definitiva, abordamos aquellos retos que limitan, dificultan o impiden nuestro [**empoderamiento**](http://civichackers.webs.upv.es/conocenos/una-aproximacion-al-concepto-de-empoderamiento/).

El [**Àrea Hackers cívics**](http://civichackers.cc) ha sido impulsada por la [**Cátedra Govern Obert**](http://www.upv.es/contenidos/CATGO/info/). Una iniciativa surgida de la colaboración entre la Concejalía de Transparencia, Gobierno Abierto y Cooperación del Ayuntamiento de València y la [Universitat Politècnica de València](http://www.upv.es).

![ÀHC](http://civichackers.webs.upv.es/wp-content/uploads/2017/02/Logo_CGO_web.png) ![ÀHC](http://civichackers.webs.upv.es/wp-content/uploads/2017/02/logo_AHC_web.png)



## Términos de uso

El contenido de este repositorio está sujeto a la licencia [GNU General Public License v3.0](https://www.gnu.org/licenses/gpl-3.0.en.html). ![](https://www.gnu.org/graphics/gplv3-127x51.png)
