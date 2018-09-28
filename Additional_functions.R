##########Funciones adicionales
##########Versión: 1.0. (control de versiones disponible en: 
##########https://github.com/daasampa)##########
##########Autor: David Arango Sampayo.##########
##########Área: Gerencia Analítica de Gestión del Fraude. 
##########Dirección de Gestión del Fraude.##########

#Conversión de los campos fecha numérico a formato fecha (date).
add_date_format = function(data, new_col, old_col){
  data[new_col] = as.Date(sapply(sapply(data[old_col], function(x){
    as.character(x)}), function(x){
      paste(substr(x, 1, 4), "-", substr(x, 5, 6), "-", substr(x, 7, 8), sep= "")
    }
  ))
  return(data)
}

#Se mide la incidencia de las características como enumeración o regeneración de 
#primera clave , entre otras, en función de fechas, con el fraude. 
valida = function(data, data_name, columns, trait, indicator){
  sql = paste("select t1.documento, t1.min_fecha_fraude, t1.max_fecha_fraude, ",
              "t2.", columns[1], ",", " t2.", columns[2],
              ", case ", 
              "when t2.documento is null then 'cliente_sin_fraude' ",
              "else 'cliente_con_fraude' ",
              "end as indicador_1, ",
              "case ", 
              "when ",
              "t2.", columns[1], " > t1.min_fecha_fraude and ", 
              "t2.", columns[2], " < t1.max_fecha_fraude ", 
              "then ", "'", trait, "_intra_fraude' ", 
              "when ", 
              "t2.", columns[1], " > t1.min_fecha_fraude and ", 
              "t2.", columns[2], " > t1.max_fecha_fraude ", 
              "then ", "'", trait, "_maxima' ", 
              "when ", 
              "t2.", columns[1], " < t1.min_fecha_fraude and ", 
              "t2.", columns[2], " > t1.max_fecha_fraude ", 
              "then ", "'fraude", "_intra_", trait, "' ", 
              "when ",
              "t2.", columns[2], " > t1.min_fecha_fraude ",
              "then ", "'", trait, "_supra_fraude' ", 
              "when ", 
              "t2.", columns[1], " < t1.min_fecha_fraude and ", 
              "t2.", columns[2], " < t1.max_fecha_fraude ", 
              "then 'fraude_maxima' ", 
              "else ", "'", trait, "_sub_fraude' ", 
              "end as indicador_2 ", 
              "from      fraudes as t1 ", 
              "left join ",
              data_name, " as t2 ",
              "on t1.documento = t2.documento ", 
              "order by t1.documento asc"
              , sep = "")
  result = sqldf(sql)
  result_to_print = list(sqldf("select indicador_1, count(documento) as 
                               total_clientes
                               from result
                               group by indicador_1
                               order by total_clientes desc"),
                         sqldf("select indicador_2, count(documento) as 
                               total_clientes
                               from result
                               where indicador_1 like '%con_fraude%'
                               group by indicador_2
                               order by total_clientes desc")
                         )
  names(result_to_print) = c("distribución_fraude", "resultado")
  ifelse(indicator == "FULL", return(result), return(result_to_print))
}