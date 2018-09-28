##########Simulaciones Montecarlo para estimacion del 
##########tiempo de primer arribo.
##########Version: 1.0. (control de versiones disponible en: 
##########https://github.com/daasampa)##########
##########Autor: David Arango Sampayo.##########
##########Area: Gerencia Analitica de Gestion del Fraude. 
##########Direccion de Gestion del Fraude.##########

#Clientes objetivo para el simulador. Se modifica la consulta para que se traiga de la tabla
#otp aquellos clientes que hayan hecho alguna vez cambio de mecanismo a ODA.
#Tiempo de ejecucion: 0.10 mins. Fecha de ejecucion: 25/09/2018#
start = Sys.time()
clientes_simulacion = sqldf(paste("with ", 
                                  "temp_1 as ( ",
                                  "select distinct t1.documento, t1.segmento ",
                                  "from clientes_objetivo_enumeracion as t1 ", 
                                  "inner join clientes_objetivo_otp as t2 ", 
                                  "on t1.documento = t2.documento ",
                                  "), ", 
                                  "temp_2 as ( ", 
                                  "select distinct documento ", 
                                  "from otp ", 
                                  "where otp = 'Cambio_mecanismo_ODA'", 
                                  ") ",
                                  "select t1.* ", 
                                  "from temp_1 as t1 ",
                                  "inner join temp_2 as t2 ", 
                                  "on t1.documento = t2.documento ", 
                                  "order by t1.documento", 
                                  sep = ""))
print(difftime(Sys.time(), start, units = "mins"))

#nsims simulaciones Montecarlo del tiempo de primer arribo al estado de fraude.
t0 = function(nsims, horizonte, cuantil, matriz, caracter){
  #Instancia de una clase markovchain que ser? eventualmente el objeto en la 
  #simulacion. 
  simulador_MC = new("markovchain", 
                     states = unlist(strsplit(caracter, " ")), 
                     byrow = TRUE, 
                     transitionMatrix = matriz)
  tau = replicate(nsims, min(which(rmarkovchain(n = horizonte, 
                                                object = simulador_MC, 
                                                t0 = "Cero", include.t0 = TRUE) == "Fraude")))
  tau = tau[is.finite(tau)]
  estadistico = floor(as.numeric(quantile(tau, cuantil)))
  return(estadistico)
}

#Se registran en el cluster los nuevos datos. 
sfExport("estados_simulacion")
sfExport("matriz_Markov")
sfClusterEval(ls())

#Se simulan los tiempos de primer arribo a estado fraude.
setwd("C:/Users/daasampa/Documents/David_Arango_Sampayo/Modelo_MC/Modelo_MC/RData")
start = Sys.time()
tiempos = sfClusterApplyLB(rep(1000, nrow(clientes_simulacion)), 
                           t0,
                           horizonte = 365, 
                           cuantil = 0.90, 
                           matriz = matriz_Markov,
                           caracter = caracter)
tiempos_primer_arribo = unlist(tiempos)
save(tiempos_primer_arribo, file = "tiempos_primer_arribo.RData", version = 3)
print(difftime(Sys.time(), start, units = "mins"))
setwd("C:/Users/daasampa/Documents/David_Arango_Sampayo/Modelo_MC/Modelo_MC/Modulos")

#se calcula el indicador de riesgo: porcentaje relativo del valor minimo del cuantil 
#del 90% del tiempo de primer arribo con respecto al cunatil del 99% todos los
#clientes.
clientes_resultado = data.frame("documento" = clientes_simulacion[, "documento"],
                                "segmento" = clientes_simulacion[, "segmento"],
                                "riesgo" = (min(tiempos_primer_arribo) / 
                                              tiempos_primer_arribo) * 
                                            100)
clientes_resultado = clientes_resultado[order(clientes_resultado[,"riesgo"], 
                                              decreasing = TRUE), ]
#Se guarda el resultado en archivos .xlsx o .csv
setwd("C:/Users/daasampa/Documents/David_Arango_Sampayo/Modelo_MC/Modelo_MC/Entregable")
write.csv(clientes_resultado, "clientes_modelo_MC.csv")
write.xlsx(clientes_resultado, "clientes_modelo_MC.xlsx")