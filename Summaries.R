##########Resumenes de datos que se usaran para mostrar en el 
##########Rmarkdown que llevar? los datos de presentacion. 
##########Version: 1.0. (control de versiones disponible en: 
##########https://github.com/daasampa)##########
##########Autor: David Arango Sampayo.##########
##########Area: Gerencia Analitica de Gestion del Fraude. 
##########Direccion de Gestion del Fraude.##########

##Descripcion de los datos:

#En aras de hacer un an?lisis preliminar, que puede ser extensible a los datos 
#adicionales de las variables todas, se crea el siguiente resumen que trae, por 
#cliente, el rango de fechas en los cuales se registr? la enumeracion.
enumeraciones = sqldf("select documento,
                      min(fecha_enumeracion) as min_fecha_enumeracion,
                      max(fecha_enumeracion) as max_fecha_enumeracion
                      from enumeracion
                      group by documento
                      order by documento asc")

#se crea el siguiente resumen que trae, por cliente, el rango de fechas en los 
#cuales se registr? una regeneraci?n de primera clave.
regeneraciones = sqldf("select documento as documento,
                       min(fecha_regeneracion_clave) as 
                       min_fecha_regeneracion,
                       max(fecha_regeneracion_clave) as 
                       max_fecha_regeneracion
                       from regeneracion
                       group by documento
                       order by documento asc")

#De forma similar al caso anterior se trae el rango de fechas de fraude.
fraudes = sqldf("select documento,
                min(fecha) as min_fecha_fraude,
                max(fecha) as max_fecha_fraude
                from fraude
                where valor <> 0
                group by documento
                order by documento asc")

#data frame en el que se presentan los datos de relacion de rangos de fechas entre 
#los eventos de enumeracion y de fraude. El campo calculado indicador_2 es el que 
#tiene toda la logica de clasificacion.

valida_enumeracion = valida(enumeracion, "enumeraciones", 
                            c("min_fecha_enumeracion", "max_fecha_enumeracion"), 
                            "enumeracion", "FULL")
valida_regeneracion = valida(regeneraciones, "regeneraciones", 
                             c("min_fecha_regeneracion", "max_fecha_regeneracion"), 
                             "regeneracion", "FULL")

fraude_enumeracion = valida_enumeracion[valida_enumeracion$indicador_1 == 
                                          'cliente_con_fraude', ]
fraude_regeneracion = valida_regeneracion[valida_regeneracion$indicador_1 == 
                                            'cliente_con_fraude', ]
#Se resumen los datos de estimación de las cadenas mediante un gráfico
#de ggplotly.
grafico_significancia = function(bool = TRUE){
  cero_plot = as.numeric(matriz_Markov["Cero", ] * 100)
  enumeracion_plot = as.numeric(matriz_Markov["Enumeracion", ] * 100)
  fraude_plot = as.numeric(matriz_Markov["Fraude", ] * 100)
  otp_plot = as.numeric(matriz_Markov["Cambio_mecanismo_ODA", ] * 100)
  
  data = data.frame(estados_simulacion, 
                    cero_plot, 
                    enumeracion_plot, 
                    fraude_plot,
                    otp_plot
                    )
  
  bar_plot = plot_ly(data, x = ~estados_simulacion, 
                     y = ~cero_plot, type = 'bar', name = 'Cero', 
                     marker = list(color = c('rgb(60, 186, 84)'))) %>% 
    add_trace(y = ~enumeracion_plot, name = 'Enumeración', marker = list(color = c('rgb(244, 194, 13)'))) %>% 
    add_trace(y = ~fraude_plot, name = 'Fraude', marker = list(color = c('rgb(219, 50, 54)'))) %>% 
    add_trace(y = ~otp_plot, name = 'Cambio_mecanismo_ODA', marker = list(color = c('rgb(72, 133, 237)'))) %>% 
    layout(xaxis = list(title = 'Estados'), 
           yaxis = list(title = 'Probabilidad (%)', range = seq(0, 110, 10)), 
           barmode = 'group')
  
  return(bar_plot)
}
grafico_significancia()


#¿Cuál ha sido el porcentaje de clientes afectados por fraude que han pasado por
#todos los estados definidos?. ¡Esta respuesta se debe señalar como resultado en
#la presentación!. Resultó de la revisión y retroalimentación con Santiago el 
#26/09/2018.

estados_busqueda = estados_simulacion[which(estados_simulacion != "Fraude")]
filas_busqueda = sapply(seq(1, length(indices_matrices)), 
                        function(j){min(
                          which(cadenas_clientes[[indices_matrices[j]]][, "cadena"] == "Fraude"))
                          })
sapply(seq(1, length(indices_matrices)), 
       function(j){
         sum(unique(cadenas_clientes[[indices_matrices[[j]]]][1:(filas_busqueda[1] - 1), 
                                                              "cadena"]) != "Cero")})
cadenas_clientes[[indices_matrices[[433]]]][1:(filas_busqueda[1] - 1), "cadena"]
