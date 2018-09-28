import pyodbc as odbc, time
start = time.time()
cnxn_lz = odbc.connect("DSN=LZ", autocommit=True)
cursor_lz = cnxn_lz.cursor()
cursor_lz.execute("drop table if exists proceso_seguridad_externa.mc_clientes_fraude purge")
cursor_lz.execute("create table if not exists proceso_seguridad_externa.mc_clientes_fraude stored as parquet as " + \
                  "with " + \
                  "temp_1 as (" + \
                  "select distinct documento " + \
                  "from proceso_seguridad_externa.mc_fraude), " + \
                  "temp_2 as ( " + \
                  "select distinct documento " + \
                  "from proceso_seguridad_externa.mc_enumeracion), " + \
                  "temp_3 as ( " + \
                  "select distinct documento " + \
                  "from proceso_seguridad_externa.mc_otp) " + \
                  "select t1.documento, " + \
                  "case " + \
                  "when t2.documento is not null then " + \
                  "'cliente_enumerado' " + \
                  "else 'cliente_no_enumerado' " + \
                  "end as indicador_enum, " + \
                  "case " + \
                  "when t3.documento is not null then " + \
                  "'cliente_otp' " + \
                  "else 'cliente_no_otp' " + \
                  "end as indicador_otp " + \
                  "from temp_1 as t1 " + \
                  "left join temp_2 as t2 " + \
                  "on t1.documento = t2.documento " + \
                  "left join temp_3 as t3 " + \
                  "on t1.documento = t3.documento"
                  )
print(str(round((time.time() - start) / 60, 2)) + " min. elapsed time")