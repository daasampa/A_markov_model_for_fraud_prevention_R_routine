##########Conexiones ODBC requeridas para consulta de datos: Medell?n, 
##########BDMINERIA, proceso_seguridad_externa##########
##########Versi?n: 1.0. (control de versiones disponible en: 
##########https://github.com/daasampa)##########
##########Autor: David Arango Sampayo.##########
##########?rea: Gerencia Anal?tica de Gesti?n del Fraude. 
##########Direcci?n de Gesti?n del Fraude.##########


#Cursor para conexi?n a la LZ.
cursor_lz = odbcConnect(dsn = "LZ")
#Cursor para conexion a Medellin ambiente productivo.
cursor_medellin = odbcConnect(dsn = "MEDELLIN_python", uid = "MCXOMONR", 
                              pwd = "DSCM0NR1")
#Cursor para conexion a Nacional ambiente productivo.
cursor_nacional = odbcConnect(dsn = "NACIONAL_python", uid = "NCXOMONR", 
                              pwd = "DSCM0NR1")
#Cursor para conexion a BDMINERIA.
cursor_bdmineria = odbcConnect(dsn = "MINERIADSC")