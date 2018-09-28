##########Sentencias SQL a ejecutar tras las conexiones ODBC
##########Son configurables de acuerdo a los valores de fechas
##########que se ingresen en la función.
##########Versión: 1.0. (control de versiones disponible en: 
##########https://github.com/daasampa)##########
##########Autor: David Arango Sampayo.##########
##########Área: Gerencia Analítica de Gestión del Fraude. 
##########Dirección de Gestión del Fraude.##########

#Datos de fraude por canales digitales reportados en SIIFRA: 
#esta función recibe como argumentos la cantidad de días 
#hacia atrás, desde la fecha actual, que se desean consultar 
#en este archivo además del objeto que almacena las características
#de la conexión ODBC.
sql_siifra = function(dias, cursor){
  fecha = gsub("-", "", as.Date(Sys.Date()) %m-% days(dias))
  sql = paste("select documento, fecha, transaccion, canal, valor ", 
              "from ", 
              "s_apoyo_corporativo.dsc_reportes_siifra_transacciones_itc ", 
              "where fecha >= ", 
              fecha,
              " and valor <> 0",
              " order by fecha asc",
              sep = "")
  siifra = sqlQuery(cursor, sql)
  return(siifra)
}

#Datos de clientes enumerados desde la regla de enumeración y 
#desde los datos que comparte diariamente TODO1.
#Los argumentos de esta función tiene la misma interpretación que
#el caso anterior.
sql_enumeracion = function(dias, cursor){
  fecha = gsub("-", "", as.Date(Sys.Date()) %m-% days(dias))
  sql = paste("select documento, fecha_enum as fecha_enumeracion, ",
              "isnull(ip, '') as ip, isnull(canal, '') as canal, ",
              "isnull(total_logueos, 0) as total_logueos, ",
              "isnull(insumo, '') as insumo ",
              "from reglas_files.consolidado_enum ", 
              "where documento is not null and ", 
              "fecha_enum is not null and fecha_enum >= ",
              fecha,
              " order by fecha_enum asc",
              sep = "")
  enumeracion = sqlQuery(cursor, query = sql)
  return(enumeracion)
} 

#Datos de clientes con acciones de regeneración de primera clave.
#Los argumentos de esta función tiene la misma interpretación que
#el caso anterior.
sql_regeneracion = function(dias, cursor){
  fecha = gsub("-", "", as.Date(Sys.Date()) %m-% days(dias))
  sql = paste("select lgnroid as documento, lgfecha as fecha_regeneracion_clave ",
              "from matlibramd.matfflogbl ",
              "where lgcdgtrn in ('22','16') and trim(lower(lgdsctrn)) ", 
              "like '%regenera%' ",
              "and lgfecha >= ",
              fecha,
              " order by lgfecha asc, lgnroid asc",
              sep = "")
  regeneracion = sqlQuery(cursor, query = sql)
  return(regeneracion)
}

#Datos de clientes con acciones de cambio de mecanismo de OTP.
#Los argumentos de esta función tiene la misma interpretación que
#el caso anterior.
sql_cambio_otp = function(dias, cursor){
  fecha = gsub("-", "", as.Date(Sys.Date()) %m-% days(dias))
  sql = paste("with ",
              "temp_1 as ( ",  
              "select cast(documento as bigint) as documento, ",
              "anotrn*10000 + mestrn*100 + diatrn as fecha_otp, ",
              "case ",
              "when lower(trim(canal)) = 'svp' and cdgtrn in (4100, 3100) then 'Inscripcion_OTP' ",
              "when lower(trim(canal)) = 'svp' and cdgtrn in (4400, 3400) then 'Cambio_mecanismo_ODA' ",
              "when lower(trim(canal)) = 'svp' and cdgtrn in (4901, 3901) then 'Actualizacion_seguridad' ",
              "when lower(trim(canal)) = 'gde' and cdgtrn = 606 then 'Inscripcion_OTP' ", 
              "when lower(trim(canal)) = 'gde' and cdgtrn = 607 then 'Cambio_mecanismo_ODA' ", 
              "when lower(trim(canal)) = 'gde' and cdgtrn = 612 then 'Actualizacion_seguridad' ", 
              "end as otp ", 
              "from s_canales.itc_itclibranl_itcffacmcn ", 
              "where ((lower(trim(canal)) = 'svp' and cdgtrn in (4100, 4400, 4901, 3100, 3400, 3901)) or ", 
              "(lower(trim(canal)) = 'gde' and cdgtrn in (606, 607, 612))) and ",
              "(cdgrpta = 0) and ", 
              "year >= year(now()) - 1 ", 
              "), ", 
              "temp_2 as ( ", 
              "select ", 
              fecha, 
              " as fecha) ", 
              " select t1.* ",
              "from temp_1 as t1 ", 
              "join temp_2 as t2 ", 
              "on t1.fecha_otp >= t2.fecha", 
              sep = "")
  
  cambio_otp = sqlQuery(cursor, sql)
  
  return(cambio_otp)
}

#Clientes activos: base con el número de identificación y segmento estructural. 
#Esta consulta procede directamente de las bases de datos almacenadas en la LZ.
#Dependiendo de cómo estén los trabajos (jobs) creados por el total de peronas que 
#están haciendo uso de las capacidades de las LZ esta consulta puede tomar un tiempo 
#considerable. Una idea que puede alivianar esta consulta es la de crear la tabla 
#desde Impala directamente y únicamente invocarla en este programa una vez esté 
#creada.
sql_clientes_activos = function(dias, cursor){
  sql = paste("with ", 
              "temp_1 as ( ", 
              "select cnnamk as llave_nombre, ", 
              "cast(cnnoss as bigint) as documento, ", 
              "cncdbi as codigo_segmento ", 
              "from s_clientes.bvclegados_visionr_cname ", 
              "where year = year(now()) and ingestion_month = ", 
              "month(now()) and ", 
              "ingestion_day = day(now()) - ", dias, 
              " and cncdst = '1' ", 
              "), ", 
              "temp_2 as ( ", 
              "select distinct xfmlcd as codigo_segmento, ", 
              "lower(trim(xfdesc)) as segmento ", 
              "from s_apoyo_corporativo.seg_visionspar_xtcod ", 
              "where year = year(now()) and ", 
              "lower(xfldnm) = 'cncdbi'", 
              ") ",
              "select t1.llave_nombre, t1.documento, ", 
              "if(regexp_like(t2.segmento, '&'), 'mi negocio',", 
              " t2.segmento) ", 
              "as segmento ", 
              "from temp_1 as t1 ", 
              "left join temp_2 as t2 ", 
              "on trim(t1.codigo_segmento) ", 
              "= trim(t2.codigo_segmento)", 
              sep = "")
  clientes_activos = sqlQuery(cursor, sql)
  return(clientes_activos)
}