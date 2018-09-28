#Modulos para conexiones a las bases de datos donde están las tablas que permiten la creación de variables.
import pyodbc as odbc, pandas as pd, time
from datetime import datetime, timedelta
start = time.time()
#Mediante la siguiente función se crea un objeto, registers, el cual contiene una lista cuyos elementos coinciden con los
#registros de la tabla descargada de la BD original. El objetivo es convertir esta lista en una cadena tal que se pueda,
#mediante la ejecución de una sentencia insert, crear una tabla que se almacena en la zona de proceso_seguridad_externa.
#Nota: este procedimiento no cuenta con el Vo.Bo. de buenas prácticas en el uso de recursos de la LZ. No obstante, considero
#que si el total de registros de la tabla a migrar no es superior a 1M, este procedimiento no "golpearía" tanto a nivel de
#procesamiento.
def registers(dataframe):
    j = 0
    registers = list()
    while j < dataframe.shape[0] - 1:
        registers.append('(' + str(list(dataframe.iloc[j])).replace('[',"").replace(']',"") + '),')
        j += 1
    else:
        registers.append('(' + str(list(dataframe.iloc[j])).replace('[',"").replace(']',"") + ')')
    return registers
#Conexión a la LZ. Parámetors ODBC.
cnxn_lz = odbc.connect("DSN=LZ", autocommit=True)
cursor_lz = cnxn_lz.cursor()
#Conexión a BDMINERIA. Parámetors ODBC.
cnxn_mineria = odbc.connect("DSN=MINERIADSC", autocommit=True)
cursor_mineria = cnxn_mineria.cursor()
#Conexión a MEDELLÍN producción. Parámetors ODBC.
cnxn_medellin = odbc.connect("DSN=MEDELLIN_python;UID=MCXOMONR;PWD=DSCM0NR1", 
                             autocommit=True)
cursor_medellin = cnxn_medellin.cursor()
#Conexión a NACIONAL. Parámetors ODBC.
cnxn_nacional = odbc.connect("DSN=NACIONAL_python;UID=NCXOMONR;PWD=DSCM0NR1", 
                             autocommit=True)
cursor_nacional = cnxn_nacional.cursor()
#Se transforman los registros de todas las tablas en una lista. A pesar que es una cantidad considerable de registros, no toma
#mucho tiempo la ejecución.
#Función para invocar los datos de enumeración.
def enumeracion(dias, connect):
    fecha = datetime.today() - timedelta(days = dias)
    fecha = str(fecha.year*10000 + fecha.month*100 + fecha.day)
    sql = "select documento, fecha_enum as fecha_enumeracion, " + \
    "cast(replace(hora_enum, ':', '') as int) as hora_enumeracion, " + \
    "isnull(ip, '') as ip, isnull(canal, '') as canal, " + \
    "isnull(total_logueos, 0) as total_logueos, " + "isnull(insumo, '') as insumo " + \
    "from reglas_files.consolidado_enum " + \
    "where documento is not null and hora_enum is not null and " + \
    "fecha_enum is not null and fecha_enum >= " + \
    fecha + " order by fecha_enum asc"
    enumeracion = pd.read_sql(sql = sql, con = connect)
    return enumeracion
#Función para invocar los datos de regeneración de primera clave.
#def regeneracion(dias, connect):
#    fecha = datetime.today() - timedelta(days = dias)
#    fecha = str(fecha.year*10000 + fecha.month*100 + fecha.day)
#    sql = "select cast(lgnroid as bigint) as documento, cast(lgfecha as int) as fecha_regeneracion_clave " + \
#    "from matlibramd.matfflogbl " + \
#    "where lgcdgtrn in ('22','16') and trim(lower(lgdsctrn)) " + "like '%regenera%' " + "and lgfecha >= " + \
#    fecha + " order by lgfecha asc, lgnroid asc"
#    regeneracion = pd.read_sql(sql = sql, con = connect)
#    return regeneracion
#Función para invocar los datos de evidente riesgoso.
def evidente(dias, connect):
    fecha = datetime.today() - timedelta(days = dias)
    fecha = str(fecha.year*10000 + fecha.month*100 + fecha.day) 
    sql = "select cast(numident as bigint) as documento, cast(substr(trim(fechorevid), 1, 8) as integer) as fechorevid " + \
    "from pcclibranl.pccfflgevi " + \
    "where (codalerta in ('1','2','3','8','9') or codvaliden = '08' or codvalcues in ('17','18','19')) " + \
    "and (fechorevid >= " + "'" + fecha + "')" + " order by numident asc"
    evidente = pd.read_sql(sql = sql, con = connect)
    return evidente
#Invocación de los datos y transformación de los mismos mediante la función registers.
#Enumeracion:
datos_enumeracion = enumeracion(365, cnxn_mineria)
registers_enumeracion = registers(datos_enumeracion)
#Evidente:
datos_evidente = evidente(365, cnxn_nacional)
registers_evidente = registers(datos_evidente)
#Regeneracion: Se cancela la invocación y escritura de esta tabla.
#En el gráfico de resumen se puede apreciar que no hay relevencia en esta variable.
#datos_regeneracion = regeneracion(365, cnxn_medellin)
#registers_regeneracion = registers(datos_regeneracion)
#Función para crear las tablas en la LZ.
def write_lz(registers, table, structurefields, fields):
    cursor_lz.execute("drop table if exists proceso_seguridad_externa." + table + " purge")
    cursor_lz.execute("create table if not exists proceso_seguridad_externa." + table + " " + structurefields)
    cursor_lz.execute("insert into table proceso_seguridad_externa." + table + " " + fields + " values " + ''.join(registers))
#Escritura en HDFS a través de la conexión ODBC a la LZ.
write_lz(registers_enumeracion, "mc_enumeracion",\
         "(documento bigint, fecha_enumeracion int, hora_enumeracion int, ip string, canal string, total_logueos int, insumo string)",\
         "(documento , fecha_enumeracion, hora_enumeracion, ip, canal, total_logueos, insumo)")
write_lz(registers_evidente, "mc_evidente", "(documento bigint, fechorevid int)", "(DOCUMENTO, FECHOREVID)")
#write_lz(registers_regeneracion, "mc_regeneracion", "(documento bigint, fecha_regeneracion_clave int)", 
#         "(DOCUMENTO, FECHA_REGENERACION_CLAVE)")
#Tablas nativas de la LZ
#Función para crear la tabla de fraudes (SIIFRA).
#Se define como hora de todos los fraudes las 23:59:59. Esto con el fin de poder
#crear las cadenas y se puedan observar los estados de OTP y de Fraude.
def fraude(dias):
    fecha = datetime.today() - timedelta(days = dias)
    fecha = str(fecha.year*10000 + fecha.month*100 + fecha.day) 
    sql = "select documento, fecha, transaccion, canal, valor, " + \
    "235959 as hora_fraude "+ \
    "from s_apoyo_corporativo.dsc_reportes_siifra_transacciones_itc " + \
    "where fecha >= " + fecha + " and valor <> 0" + " order by fecha asc"
    cursor_lz.execute("drop table if exists proceso_seguridad_externa.mc_fraude purge")
    cursor_lz.execute("create table if not exists proceso_seguridad_externa.mc_fraude stored as parquet as " + sql)    
#Se ejecuta la consulta.
fraude(365)
#Función para crear la tabla de OTP (ITCFFACMCN).
def otp(dias):
    fecha = datetime.today() - timedelta(days = dias)
    fecha = str(fecha.year*10000 + fecha.month*100 + fecha.day) 
    sql = "with " + \
    "temp_1 as ( " + \
    "select cast(documento as bigint) as documento, " + \
    "anotrn*10000 + mestrn*100 + diatrn as fecha_otp, " + \
    "cast(substring(lpad(cast(horatrn as string), 8, '0'), 1, 6) as int) as hora_otp, "+ \
    "case " + \
    "when lower(trim(canal)) = 'svp' and cdgtrn in (4100, 3100) then 'Inscripcion_OTP' " + \
    "when lower(trim(canal)) = 'svp' and cdgtrn in (4400, 3400) then 'Cambio_mecanismo_ODA' " + \
    "when lower(trim(canal)) = 'svp' and cdgtrn in (4901, 3901) then 'Actualizacion_seguridad' " + \
    "when lower(trim(canal)) = 'gde' and cdgtrn = 606 then 'Inscripcion_OTP' " + \
    "when lower(trim(canal)) = 'gde' and cdgtrn = 607 then 'Cambio_mecanismo_ODA' " + \
    "when lower(trim(canal)) = 'gde' and cdgtrn = 612 then 'Actualizacion_seguridad' " + \
    "end as otp " + \
    "from s_canales.itc_itclibranl_itcffacmcn " + \
    "where ((lower(trim(canal)) = 'svp' and cdgtrn in (4100, 4400, 4901, 3100, 3400, 3901)) or " + \
    "(lower(trim(canal)) = 'gde' and cdgtrn in (606, 607, 612))) and " + \
    "(cdgrpta = 0) and " + \
    "year >= year(now()) - 1 " + \
    "), " + \
    "temp_2 as ( " + \
    "select " + \
    fecha + \
    " as fecha) " + \
    "select t1.* " + \
    "from temp_1 as t1 " + \
    "join temp_2 as t2 " + \
    "on t1.fecha_otp >= t2.fecha"
    cursor_lz.execute("drop table if exists proceso_seguridad_externa.mc_otp purge")
    cursor_lz.execute("create table if not exists proceso_seguridad_externa.mc_otp stored as parquet as " + sql)    
#Se ejecuta la consulta.
otp(365)
#Tiempo total transcurrido
print(str(round((time.time() - start) / 60, 2)) + " min. elapsed time")