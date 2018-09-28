###Carga de módulos y conexiones ODBC. 
source("Modules.R")
source("Connections.R")

###Datos invocados: fuente SQL_statements.R. 
###Tiempo de ejecucion: 0.45 mins. Fecha de ejecucion: 19/09/2018###
start = Sys.time()
fraude = sqlQuery(cursor_lz, "select * from proceso_seguridad_externa.mc_fraude")
enumeracion = sqlQuery(cursor_lz, "select * from proceso_seguridad_externa.mc_enumeracion")
otp = sqlQuery(cursor_lz, "select * from proceso_seguridad_externa.mc_otp")
#regeneracion = sqlQuery(cursor_lz, "select * from proceso_seguridad_externa.mc_regeneracion")
#evidente = sqlQuery(cursor_lz, "select * from proceso_seguridad_externa.mc_evidente")
print(difftime(Sys.time(), start, units = "mins"))

###Datos con modificacion del campo fecha: fuente Additional_functions.R###
###Tiempo de ejecucion: 1.04 mins. Fecha de ejecucion: 19/09/2018###
source("Additional_functions.R")
start = Sys.time()
fraude = add_date_format(fraude, "fraud_date", 
                                 "fecha")
enumeracion = add_date_format(enumeracion, "enum_date", 
                                           "fecha_enumeracion")
#regeneracion = add_date_format(regeneracion, "regen_date", 
#                                             "fecha_regeneracion_clave")
otp = add_date_format(otp, "otp_date", 
                           "fecha_otp")
#evidente = add_date_format(evidente, "evid_date", 
#                           "fechorevid")
print(difftime(Sys.time(), start, units = "mins"))

#Estructura de datos tras modificaci?n.
#sapply(regeneracion, class)
sapply(enumeracion, class)
sapply(otp, class)
#sapply(evidente, class)
sapply(fraude, class)

#Identificacion de clientes activos en el CNAME ingestado en la LZ.
#Puede ocurrir que no se haya ingestado la tabla y el argumento
#de la funcion no sea 1, sino, por ejemplo, 2, 3.
#Tiempo de ejecucion: 2.70 mins. Fecha de ejecucion: 29/08/2018#
source("SQL_statements.R")
start = Sys.time()
clientes_activos = sql_clientes_activos(1, cursor_lz)
print(difftime(Sys.time(), start, units = "mins"))

#Se almacenan los datos de clientes afectados por fraude y se especifica si tienen 
#o no la caracteristica de enumeracion y regeneracion de primera clave.
#Tiempo de ejecucion: 0.02 mins. Fecha de ejecuci?n: 19/09/2018#
start = Sys.time()
clientes_fraude = sqlQuery(cursor_lz, "select * from proceso_seguridad_externa.mc_clientes_fraude")
print(difftime(Sys.time(), start, units = "mins"))

#Esta funcion permite conocer qui?nes de los clientes activos y que han presentado
#una determinada caracter?stica: enumeraci?n, regeneraci?n de primera clave,
#cambio de mecanismo de otpentre otras, no han tenido fraude. 
#estos seran al base objetivo para el pron?stico mediante la simulaci?n de las cadenas.
clientes_objetivo = function(base, caracteristica, resultado){
  temp_1 = sqldf(paste("with ",
                       "tabla_1 as ( ", 
                       "select distinct documento as documento from ", 
                       base, 
                       ") ", 
                       "select t1.documento, t1.segmento, ", 
                       "case ", 
                       "when t2.documento is null ", 
                       "then 'cliente_sin_", 
                       caracteristica, "' ", 
                       "else 'cliente_con_", 
                       caracteristica, "' ", 
                       "end as indicador ", 
                       "from clientes_activos as t1 ", 
                       "left join tabla_1 as t2 ", 
                       "on t1.documento = t2.documento ", 
                       "order by t1.documento", 
                       sep = ""))
  
  temp_2 = sqldf(paste("with ", 
                       "tabla_2 as ( ", 
                       "select documento , segmento ", 
                       "from temp_1 where indicador = 'cliente_con_", 
                       caracteristica, "') ", 
                       "select t1.documento, t1.segmento, ",  
                       "case ", 
                       "when t2.documento is null ", 
                       "then 'cliente_", 
                       caracteristica, "_sin_fraude' ", 
                       "else 'cliente_", 
                       caracteristica, "_con_fraude' ", 
                       "end as indicador ", 
                       "from tabla_2 as t1 ", 
                       "left join clientes_fraude as t2 ", 
                       "on t1.documento = t2.documento ",
                       "where indicador = 'cliente_",
                       caracteristica, "_sin_fraude'",
                       "order by t1.documento", 
                       sep = ""))
  
  resumen = sqldf("select indicador, count(documento) as frecuencia 
                  from temp_2 
                  group by indicador
                  order by indicador asc")
  
  if(resultado == "detalle"){
    return(temp_2)
  }else{
    if(resultado == "resumen"){
      return(resumen) 
    }
    else print("Error: valor no v?lido para el par?metro")
  }
}

#Cuantos de los clientes activos del Banco han tenido enumeraci?n o regeneraci?n
#y no han tenido fraude?. 
clientes_objetivo("enumeracion", "enumeracion", "resumen")
#clientes_objetivo("regeneracion", "regeneracion", "resumen")
clientes_objetivo("otp", "otp", "resumen")
#clientes_objetivo("evidente", "evidente", "resumen")

#Tiempo de ejecucion: 4.71. mins. Fecha de ejecucion: 19/09/2018#
start = Sys.time()
clientes_objetivo_enumeracion = clientes_objetivo("enumeracion", 
                                                  "enumeracion", 
                                                  "detalle")
#clientes_objetivo_regeneracion = clientes_objetivo("regeneracion", 
#                                                   "regeneracion", 
#                                                   "detalle")
clientes_objetivo_otp = clientes_objetivo("otp", 
                                          "otp", 
                                          "detalle")
#clientes_objetivo_evidente = clientes_objetivo("evidente", 
#                                               "evidente", 
#                                               "detalle")
print(difftime(Sys.time(), start, units = "mins"))

head(clientes_objetivo_enumeracion)
#head(clientes_objetivo_regeneracion)
head(clientes_objetivo_otp)
#head(clientes_objetivo_evidente)

#Se escogen los clientes que han tenido tanto enumeracion como regeneracion.
#Tiempo de ejecucion: 0.001 mins. Fecha de ejecucion: 19/09/2018#
start = Sys.time()
clientes_factores = sqldf(paste("select documento from clientes_fraude where ", 
                                "indicador_enum = 'cliente_enumerado' and ", 
                                #"indicador_regen = 'cliente_regeneracion' and ",
                                "indicador_otp = 'cliente_otp'",
                                #"indicador_evidente = 'cliente_evidente'",
                                "where ", 
                                 sep = ""))
print(difftime(Sys.time(), start, units = "mins"))

###Ambiente cargado: fuente Cluster_configuration.R###
source("Cluster_configuration.R")
cluster(8)
#sfStop()
###Funcion invocada: fuente Markov_Chains.R###
source("Markov_chains.R")

###Estimacion de las matrices de transicion y de las cadenas: 
###fuente Chains_estimation.R###
#Tiempo de ejecucion: 29.67 mins (s**t). Fecha de ejecucion: 12/09/2018#
source("Chains_estimation.R")


###Se preparan los datos para la simulación montecarlo basado en la 
###"matriz ensamblada".

#Total de estados de transición contemplados en el modelo.
estados_transicion = c("Actualizacion_seguridad", "Cambio_mecanismo_ODA", "Cero", "Enumeracion", 
                       "Fraude", "Inscripcion_OTP")
#Este dataframe contiene la distribucion del numero
#de estados de las matrices estimadas.
data.frame("numero_estados" = seq(1, length(estados_transicion)), 
           "numero_matrices" = sapply(seq(1, length(estados_transicion)), 
                                      function(j){sum(sapply(seq(1, length(matrices_transicion)), 
                                                             function(i){
                                                               ncol(matrices_transicion[[i]])}) 
                                                      == j)}))

#Esta funcion retorna las posiciones dentro de la lista de matrices que contienen un numero determinado 
#de estados.
indices = function(x){
  which(unlist(sapply(seq(1, length(matrices_transicion)), 
                      function(i){length(which(ncol(matrices_transicion[[i]]) == x))})) == 1)
}

#Se conoce, dentro de cada grupo definido por el n?mero de estados, cuales son los estados mas 
#frecuentes?.
consulta = data.frame("estados" = unlist(lapply(indices(4), 
                                                function(i){
                                                  colnames(matrices_transicion[[i]])
                                                })))
sqldf("select estados, count(*) as frecuencia 
      from consulta group by estados 
      order by frecuencia desc, estados asc")

#Se calculan los indices en la lista de matrices que tienen los estados especificados como una cadena
#de caracteres. Recordar que las columnas de las matrices de transicion estan ordenadas alfabeticamente.
seleccion_matrices = function(caracter){
  which(sapply(seq(1, length(matrices_transicion)), 
               function(i){
                 paste0(colnames(matrices_transicion[[i]]), collapse = " ") == 
                   caracter
               }))
}

seleccion_matrices("Cambio_mecanismo_ODA Cero Enumeracion Fraude")

matriz_transicion = function(cuantil, caracter){
  transiciones = permutations(n = length(unlist(strsplit(caracter, " "))), r = 2, 
                              v = unlist(strsplit(caracter, " ")), 
                              repeats.allowed = TRUE)
  
  #Se pueden consultar las componentes de alguna de las matrices de transici?n en la 
  #lista global.
  ensamblador = function(cliente, fila){
    matrices_transicion[[cliente]][transiciones[fila, 1], transiciones[fila, 2]]
  }
  
  #?ndices en los cuales se debe consultar la lista de matrices de transici?n. 
  #Puede ocurrir que la matriz
  #estimada no tenga todos los estados necesarios.
  indices = seleccion_matrices(caracter)
  
  #Para cada una de las probabilidades de transici?n en la lista de matrices se 
  #calculan los cuantiles se?alados.
  ensamble = function(j){
    quantile(sapply(indices, function(x){
      ensamblador(x, j)}),
      c(0.500, 0.750, 0.800, 0.850, 0.875, 0.900, 0.950, 
        0.955, 0.960, 0.965, 0.970, 0.975, 0.980, 0.985, 
        0.990))
  }
  
  #Resultado: cuantiles de cada una de las probabilidades de transici?n.
  #Hacer esto es una forma de "ensamblar" las matrices tal que se tenga una ?nica 
  #matriz P de transaci?n para
  #las simulaciones.
  descripcion = sapply(seq(1, nrow(transiciones)), function(x){ensamble(x)})
  colnames(descripcion) = sapply(seq(1, nrow(transiciones)), 
                                 function(x){
                                   paste(transiciones[x, 1], "-", 
                                         transiciones[x, 2], 
                                         sep = "")
                                 })
  
  #Se selecciona un cuantil particular.
  descripcion[cuantil, ]
  P = matrix(data = descripcion[cuantil, ], byrow = TRUE, nrow = length(unlist(strsplit(caracter, " "))))
  
  #Se escala la matriz tal que sea en realidad una matriz de transici?n.
  P = t(sapply(seq(1, length(rowSums(P))), function(i){P[i, ] / rowSums(P)[i]}))
  
  #Se rotula la matriz de transici?n
  rownames(P) = unlist(strsplit(caracter, " "))
  colnames(P) = unlist(strsplit(caracter, " "))
  
  return(P)
}

percentil = "95%"
matriz_Markov = matriz_transicion(percentil, "Cambio_mecanismo_ODA Cero Enumeracion Fraude")
estados_simulacion = unlist(strsplit("Cambio_mecanismo_ODA Cero Enumeracion Fraude", " "))
caracter = "Cambio_mecanismo_ODA Cero Enumeracion Fraude"

#Ejecución de la simulaciones.
source("Montecarlo_simulations.R")

###Se graba el total del ambiente cargado en la sesion.
start = Sys.time()
save.image()
print(difftime(Sys.time(), start, units = "mins"))

###Tiempo total de ejecuci?n: 67.843 mins. Fecha de ejecuci?n: 29/08/2018#