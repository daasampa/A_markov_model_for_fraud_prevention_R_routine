##########Configuraci?n para ejecuci?n en paralelo de la
##########estimaci?n de las cadenas.
##########Versi?n: 1.0. (control de versiones disponible en: 
##########https://github.com/daasampa)##########
##########Autor: David Arango Sampayo.##########
##########?rea: Gerencia Anal?tica de Gesti?n del Fraude. 
##########Direcci?n de Gesti?n del Fraude.##########

#Se inicia el computo en paralelo con los 4 procesadores.
#Se cargan las librer?as y los datos.
cluster = function(cpus){
  sfInit(parallel = TRUE, cpus, type = "SOCK") 
  return(list(sfLibrary(markovchain), 
              sfLibrary(sqldf),
              sfExport("clientes_factores"), 
              sfExport("enumeracion"),
              sfExport("otp"),
              sfExport("fraude"), 
              sfClusterEval(ls())))
}