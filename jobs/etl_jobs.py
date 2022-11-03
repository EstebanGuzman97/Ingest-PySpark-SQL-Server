from dependencies.config import Config
import dependencies.spark as spark
from dependencies import logging
from dist import password as p
import os


class EtlJobs:
    name_table = ""

    def __init__(self, name_table, Config):
        global conf
        self.name_table = name_table
        conf = Config

    def extract_data(self, spark):

        server = conf.get_server()
        database = conf.get_name_database()
        table = self.name_table
        user = conf.get_host_user()
        password = p.Password.get_pass()
        DRIVERCLASS = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        URL = f'jdbc:sqlserver://{server};databaseName={database}'
        query = f'SELECT * FROM {table}'  # Aplicar filtro con WHERE para pruebas, no traer tabla completa.

        try:
            print("Generando Sesión con SQL...")

            jdbcDF = spark.read.format('jdbc') \
                .option('url', URL) \
                .option('user', user) \
                .option('password', password) \
                .option('driver', DRIVERCLASS) \
                .option('query', query) \
                .load()

            # jdbcDF.show()
            # jdbcDF.printSchema()

        except ValueError as error:
            logging.info("Fallo conexión del conector JDBC con la DB.", error)

        return jdbcDF

    def transform_data(self, data_extract):
        print(f'Nombres: {self.name_table}')

        if self.name_table == 'contrato':
            data_transform = spark.change_type_column_contrato(data_extract)
            return data_transform

        if self.name_table == 'envase':
            data_transform = spark.change_type_column_envase(data_extract)
            return data_transform

        if self.name_table == 'equipo':
            data_transform = spark.change_type_column_equipo(data_extract)
            return data_transform

        if self.name_table == 'equipos':
            data_transform = spark.change_type_column_equipos(data_extract)
            return data_transform

        if self.name_table == 'evento':
            data_transform = spark.change_type_column_evento(data_extract)
            return data_transform

        if self.name_table == 'evento_serviciotransporte':
            data_transform = spark.change_type_column_evento_servicio_transporte(data_extract)
            return data_transform

        if self.name_table == 'flota':
            data_transform = spark.change_type_column_flota(data_extract)
            return data_transform

        if self.name_table == 'mat_material':
            data_transform = spark.change_type_column_mat_material(data_extract)
            return data_transform

        if self.name_table == 'mat_materialconversionmedida':
            data_transform = spark.change_type_column_mat_materialconversionmedida(data_extract)
            return data_transform

        if self.name_table == 'ordentrabajoarenacantidad':
            data_transform = spark.change_type_column_orden_trabajo_arena_cantidad(data_extract)
            return data_transform

        if self.name_table == 'ordentrabajotransporte':
            data_transform = spark.change_type_column_orden_trabajo_transporte(data_extract)
            return data_transform

        if self.name_table == 'ordentrabajotransporteestado':
            data_transform = spark.change_type_column_orden_trabajo_transporte_estado(data_extract)
            return data_transform

        if self.name_table == 'pozo':
            data_transform = spark.change_type_column_pozo(data_extract)
            return data_transform

        if self.name_table == 'proveedor':
            data_transform = spark.change_type_column_proveedor(data_extract)
            return data_transform

        if self.name_table == 'servicio':
            data_transform = spark.change_type_column_servicio(data_extract)
            return data_transform

        if self.name_table == 'serviciotransporte':
            data_transform = spark.change_type_column_servicio_transporte(data_extract)
            return data_transform

        if self.name_table == 'serviciotransporte_tiposusos':
            data_transform = spark.change_type_column_servicio_transporte_tipos_usos(data_extract)
            return data_transform

        if self.name_table == 'serviciotransportecategoria':
            data_transform = spark.change_type_column_servicio_transporte_categoria(data_extract)
            return data_transform

        if self.name_table == 'serviciotransportetipocarga':
            data_transform = spark.change_type_column_servicio_transporte_tipo_carga(data_extract)
            return data_transform

        if self.name_table == 'solicitud_arena_malla':
            data_transform = spark.change_type_column_solicitud_arena_malla(data_extract)
            return data_transform

        if self.name_table == 'solicitudtransporte':
            data_transform = spark.change_type_column_solicitud_transporte(data_extract)
            return data_transform

        if self.name_table == 'solicitudtransporteestado':
            data_transform = spark.change_type_column_solicitud_transporte_estado(data_extract)
            return data_transform

        if self.name_table == 'solicitudtransportehistorial':
            data_transform = spark.change_type_column_solicitud_transporte_historial(data_extract)
            return data_transform

        if self.name_table == 'tipocarga':
            data_transform = spark.change_type_column_solicitud_tipo_carga(data_extract)
            return data_transform

        if self.name_table == 'tipocargavehiculo':
            data_transform = spark.change_type_column_solicitud_tipo_carga_vehiculo(data_extract)
            return data_transform

        if self.name_table == 'tiposusos':
            data_transform = spark.change_type_column_tipos_usos(data_extract)
            return data_transform

        if self.name_table == 'tsl_sala':
            data_transform = spark.change_type_column_tsl_sala(data_extract)
            return data_transform

        if self.name_table == 't_cabhabilitas':
            data_transform = spark.change_type_column_t_cabhabilitas(data_extract)
            return data_transform

        if self.name_table == 't_dethabilitas':
            data_transform = spark.change_type_column_t_dethabilitas(data_extract)
            return data_transform

        if self.name_table == 'unidades':
            data_transform = spark.change_type_column_unidades(data_extract)
            return data_transform

        if self.name_table == 'unidadnegocio':
            data_transform = spark.change_type_column_unidad_negocio(data_extract)
            return data_transform

        if self.name_table == 'usuario':
            data_transform = spark.change_type_column_usuario(data_extract)
            return data_transform

        if self.name_table == 'usuario_unidadnegocio':
            data_transform = spark.change_type_column_usuario_unidad_negocio(data_extract)
            return data_transform

        if self.name_table == 'vehiculo_flota':
            data_transform = spark.change_type_column_vehiculo_flota(data_extract)
            return data_transform

        if self.name_table == 'vehiculocategoria':
            data_transform = spark.change_type_column_vehiculo_categoria(data_extract)
            return data_transform

        if self.name_table == 'vehiculocategorialog':
            data_transform = spark.change_type_column_vehiculo_categoria_log(data_extract)
            return data_transform

        if self.name_table == 'vehiculos':
            data_transform = spark.change_type_column_vehiculos(data_extract)
            return data_transform

        if self.name_table == 'vehiculoseguimiento':
            data_transform = spark.change_type_column_vehiculo_seguimiento(data_extract)
            return data_transform

        if self.name_table == 'vehiculotarea':
            data_transform = spark.change_type_column_vehiculo_tarea(data_extract)
            return data_transform

        if self.name_table == 'vehiculotipo':
            data_transform = spark.change_type_column_vehiculo_tipo(data_extract)
            return data_transform

        if self.name_table == 'subzonasinteres':
            data_transform = spark.change_type_column_subzonas_interes(data_extract)
            return data_transform

        if self.name_table == 'zonasinteres':
            data_transform = spark.change_type_column_zonas_interes(data_extract)
            return data_transform

        if self.name_table == 'zonasinterestipos':
            data_transform = spark.change_type_column_zonas_interes_tipos(data_extract)
            return data_transform

        if self.name_table == 'sitrack_report_detenido':
            data_transform = spark.change_type_column_sitrack_report_detenido(data_extract)
            return data_transform

        return data_extract

    def load_data(self, data_transformed):

        path = conf.get_path_hdfs_output()
        path_stg = "raw_data"
        path_output_stg = os.path.join(path, self.name_table.lower(), path_stg, "")

        print(f'Path sin particionar:{path_output_stg}')

        try:
            data_transformed.write.save(path=path_output_stg, format="parquet", mode="overwrite")

        except ValueError as error:
            logging.error("No se pudo escribir en HDFS.", error)
            logging.info('Escritura en HDFS correcta')

        '''
        try:
          data_transformed.repartition(1) \
          .write.format("parquet") \
          .option("charset", "utf-8") \
          .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss") \
          .partitionBy("Fecha_Stock_Dttm") \
          .mode("overwrite") \
          .save(path_output_particioned) \
    
    
        except ValueError as error:
          logging.error("No se pudo escribir en HDFS.", error)
          logging.info('Escritura en HDFS correcta')
        '''