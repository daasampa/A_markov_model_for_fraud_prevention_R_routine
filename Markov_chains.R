##########Creacion de los datos necesarios para la
##########Estimacion de cadenas de Markov.
##########Version: 1.0. (control de versiones disponible en: 
##########https://github.com/daasampa)##########
##########Autor: David Arango Sampayo.##########
##########Area: Gerencia Analitica de Gestion del Fraude. 
##########Direccion de Gestion del Fraude.##########

#Funcion que se encarga de la generacion de secuencias de estados para cada cliente
#y eventualmente estima,  para cada clientes, una matriz de transicion a traves del
#metodo que se le especifique.

estima_cadena_cliente = function(cliente_index, m, resultado, metodo, hiper){
  #Secuencialmente se escoge el documento de cada cliente.
  cliente_id = as.data.frame(clientes_factores[cliente_index, ])
  colnames(cliente_id) = c("documento")
  
  #Fechas en las que el cliente ha tenido fraude.
  fraude_factores = sqldf(paste("with " , 
                                "temp_1 as ( " , 
                                "select t1.documento, t2.fraud_date, t2.hora_fraude ", 
                                "from cliente_id as t1 ", 
                                "inner join fraude as t2 ", 
                                "on t1.documento = t2.documento ",
                                "order by t1.documento asc, t2.fraud_date asc ", 
                                ") " , 
                                "select distinct * from temp_1 " , 
                                sep = ""))
  
  #Fechas en las que el cliente ha tenido enumeracion.
  enumeracion_factores = sqldf(paste("with " , 
                                     "temp_1 as ( " , 
                                     "select t1.documento, t2.enum_date, t2.hora_enumeracion ", 
                                     "from cliente_id as t1 ", 
                                     "inner join enumeracion as t2 ", 
                                     "on t1.documento = t2.documento ",
                                     "order by t1.documento asc, t2.enum_date asc ", 
                                     ") " , 
                                     "select distinct * from temp_1 " , 
                                     sep = ""))
  
  #Fechas en las que el cliente ha tenido regeneracion de primera clave.
  #Se remueve la tabla de regeneraciones de primera clave.
  #regeneracion_factores = sqldf(paste("with " , 
  #                                    "temp_1 as ( " , 
  #                                    "select t1.documento, t2.regen_date ", 
  #                                    "from cliente_id as t1 ", 
  #                                    "inner join regeneracion as t2 ", 
  #                                    "on t1.documento = t2.documento ",
  #                                    "order by t1.documento asc, ", 
  #                                    "t2.regen_date asc ", 
  #                                    ") " , 
  #                                    "select distinct * from temp_1 ",  
  #                                    sep = ""))
  
  #Fechas en las que el cliente ha tenido eventos relacionados con OTP
  otp_factores = sqldf(paste("with " , 
                             "temp_1 as ( " , 
                             "select t1.documento, t2.otp_date, t2.otp, t2.hora_otp ", 
                             "from cliente_id as t1 ", 
                             "inner join otp as t2 ", 
                             "on t1.documento = t2.documento ",
                             "order by t1.documento asc, ", 
                             "t2.otp_date asc ", 
                             ") " , 
                             "select distinct * from temp_1 ", 
                             sep = ""))
  #Fechas en las que los clientes han tenido un registro de evidente riesgoso.
  #Se remueve la tabla de regeneraciones de primera clave.
  #evidente_factores = sqldf(paste("with " , 
  #                                "temp_1 as ( " , 
  #                                "select t1.documento, t2.evid_date ", 
  #                                "from cliente_id as t1 ", 
  #                                "inner join evidente as t2 ", 
  #                                "on t1.documento = t2.documento ",
  #                                "order by t1.documento asc, ", 
  #                                "t2.evid_date asc ", 
  #                                ") " , 
  #                                "select distinct * from temp_1 ", 
  #                                sep = ""))
  
  #Fecha minima en la cual comenzara la secuencia de estados para el cliente.
  infimo = min(min(fraude_factores[, "fraud_date"]),
               min(enumeracion_factores[, "enum_date"]),
               min(otp_factores[, "otp_date"])
               #min(regeneracion_factores[, "regen_date"]),
               #min(evidente_factores[, "evid_date"])
               )
  
  #Fecha maxima en la cual comenzar? la secuencia de estados para el cliente.
  #La variable m se adiciona para que las cadenas no terminen en estados únicos,
  #es decir, que, por ejemplo, el estado Fraude no sea el único y último estado.
  #Esto es importante tenerlo debido a que hay ocasiones la matriz de transición
  #estimada tiene un estado que no suma 100% en sus probabilidades.
  supremo = max(max(fraude_factores[, "fraud_date"]),
                max(enumeracion_factores[, "enum_date"]),
                max(otp_factores[, "otp_date"]) 
                #max(regeneracion_factores[, "regen_date"]),
                #max(evidente_factores[, "evid_date"])
                ) + m
  
  #dataframe con la identificacion del cliente y con la secuencia de fechas sobre 
  #la que se construir la cadena.
  secuencia_fecha = data.frame(cliente_id,
                               "fecha" = seq(infimo, supremo, "days"))
  
  secuencia_hora = data.frame(cliente_id,
                              "hora_inicio" = seq(0, 180000, by = 60000),
                              "hora_fin" = seq(60000, 240000, by = 60000))
  
  secuencia = sqldf(paste("select t1.documento, t1.fecha, t2.hora_inicio, t2.hora_fin ", 
                          "from secuencia_fecha as t1 ",
                          "join secuencia_hora as t2 ",
                          "on t1.documento = t2.documento ",
                          "order by t1.fecha asc, t2.hora_inicio asc, t2.hora_fin asc",
                          sep= ""))
  
  #se construye la evolucion, medida en dias, del cliente; para cada fecha, desde el 
  #infimo hasta el supremo, se puede observar que estado tuvo el cliente ese dia.
  #El orden de la sentecia en el case infuye demasiado en el calculo de las cadenas
  #debido a que si varios eventos se presentaron el mismo dia, unicamente se registrar
  #el primer when.
  cadena_cliente = sqldf(paste("select t1.documento, ", "t1.fecha, t1.hora_inicio, t1.hora_fin, ",
                               "case ", 
                               "when t4.documento is not null then t4.otp ", 
                               "when t3.documento is not null then 'Enumeracion' ",
                               "when t2.documento is not null then 'Fraude' ", 
                               "else 'Cero' ", 
                               "end as cadena ", 
                               "from secuencia as t1 ", 
                               "left join fraude_factores as t2 ", 
                               "on t1.documento = t2.documento and ", 
                               "t1.fecha = t2.fraud_date and ", 
                               "t1.hora_inicio <= t2.hora_fraude and t2.hora_fraude <= t1.hora_fin ", 
                               "left join enumeracion_factores as t3 ", 
                               "on t1.documento = t3.documento and ", 
                               "t1.fecha = t3.enum_date and ",
                               "t1.hora_inicio <= t3.hora_enumeracion and t3.hora_enumeracion <= t1.hora_fin ", 
                               "left join otp_factores as t4 ", 
                               "on t1.documento = t4.documento and ", 
                               "t1.fecha = t4.otp_date and ",
                               "t1.hora_inicio <= t4.hora_otp and t4.hora_otp <= t1.hora_fin ", 
                               "order by t1.fecha asc, t1.hora_inicio asc, t1.hora_fin asc",
                               sep = ""))
  #Resumen por cliente del numero de veces en las que, por d?a, tuvo transiciones 
  #desde cada uno de los estados.
  #transiciones = createSequenceMatrix(stringchar = cadena_cliente$cadena)
  
  #Estimacion de la cadena de Markov. Por defecto el metodo de estimacion de las 
  #tasas de transicion es el metodo de maxima verosimilitud.
  cadena_markov = if(metodo == "mle"){
    markovchainFit(data = cadena_cliente$cadena, method = metodo, parallel = TRUE, 
                   confidencelevel = 0.90)
  }else{
    markovchainFit(data = cadena_cliente$cadena, method = metodo, parallel = TRUE, 
                   confidencelevel = 0.90, hyperparam = hiper)
  }
  
  #Se extrae del objeto la matriz de transici?n o la cadena para cada cliente.
  if(resultado == "cadena"){
    return(cadena_cliente)
  }else{
    return(cadena_markov$estimate[,])
  }
}