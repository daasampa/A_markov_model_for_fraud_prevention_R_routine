##########Estimacion de las matrices de transicion.
##########Version: 1.0. (control de versiones disponible en: 
##########https://github.com/daasampa)##########
##########Autor: David Arango Sampayo.##########
##########Area: Gerencia Analitica de Gestion del Fraude. 
##########Direccion de Gestion del Fraude.##########

#Medicion del tiempo de calculo: tiempos variables que dependen ademas de la maquina 
#en donde se ejecuten, pero de los ensayos que he hecho tengo reportes de menos de 
#una hora.
start = Sys.time()
setwd("C:/Users/daasampa/Documents/David_Arango_Sampayo/Modelo_MC/Modelo_MC/RData")
apriori = matrix(c(1, 1, 2, 
                   3, 2, 1, 
                   2, 2, 3), 
                 nrow = 3, byrow = TRUE)

matrices_transicion = sfClusterApplyLB(seq(1, nrow(clientes_factores)), 
                                       estima_cadena_cliente, 
                                       10,
                                       "matrices",
                                       "mle", 
                                       FALSE)

cadenas_clientes = sfClusterApplyLB(seq(1, nrow(clientes_factores)), 
                                    estima_cadena_cliente, 
                                    10,
                                    "cadena", 
                                    "mle", 
                                    FALSE)

#Se guardan en un archivo de extensi?n .RData con los datos de la lista 
#matrices_transicion. El objetivo de esto es poder guardar los datos calculados para
#una eventual invocaci?n.
save(matrices_transicion, file = "matrices_transicion.RData", version = 3)
save(cadenas_clientes, file = "cadenas_clientes.RData", version = 3)
setwd("C:/Users/daasampa/Documents/David_Arango_Sampayo/Modelo_MC/Modelo_MC/Modulos")
print(difftime(Sys.time(), start, units = "mins"))