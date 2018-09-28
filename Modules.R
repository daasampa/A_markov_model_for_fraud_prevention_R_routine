##########M?dulos requeridos para: tratamientos de datos y estimaci?n de las cadenas, 
##########entre otros.##########
##########Versi?n: 1.0. (control de versiones disponible en: 
##########https://github.com/daasampa)##########
##########Autor: David Arango Sampayo.##########
##########?rea: Gerencia Anal?tica de Gesti?n del Fraude. 
##########Direcci?n de Gesti?n del Fraude.##########


#M?dulo RODBC: contiene las clases necesarias para las conexiones a bases de datos 
#relacionales.
#Se pueden descargar tablas y almacenarlas en dataframes, a parte de poder lanzar 
#sentencias SQL para la creaci?n de nuevas tablas en los esquemas de la 
#bases de datos.
#La base que se consulta es: proceso_seguridad_externa.
require(RODBC)
#M?dulo sqldf: permite hacer trtatamiento de los datos almacenados en un dataframe 
#mediante comandos SQL.
require(sqldf)
#M?dulo markovchain: contiene las rutinas para estimaci?n y simulaci?n de una 
#cadena de Markov.
require(markovchain)
#M?dulo snowfall: permite hacer operaciones en paralelo habilitando un 
#"cluster" donde los "esclavos" son los procesadores de la m?quina y se hace uso 
#de la memoria RAM.
require(snowfall)
#M?dulo gtools: contiene, entre otras funciones, una que permite hacer 
#permiutaciones de objetos.
require(gtools)
#M?dulo ggplot2 y plotly: contienen herramientas para crear gr?ficos de alta calidad.
require(ggplot2)
require(plotly)
#M?dulo lubridate: permite el manejo de datos tipo fecha (date).
require(lubridate)
#M?dulo xlsx: permite exportar datos del ambiente a arrchivos .xlsx.
require(xlsx)