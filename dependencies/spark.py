from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf, SQLContext
from dependencies.config import Config
from pyspark.sql.types import *


# conf = SparkConf().setAppName("PAS/TSL") \
#       .set("spark.driver.extraClassPath","jars/mssql-jdbc-11.2.0.jre8.jar")

def get_spark_session(conf_read):
    conf = SparkConf() \
        .setAppName("test_pas_tsl") \
        .setMaster("yarn") \
        .set("spark.executor.memory", "40g") \
        .set("spark.executor.cores", "2") \
        .set("spark.driver.extraClassPath", 'mssql-jdbc-11.2.0.jre8.jar')
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    spark = sqlContext.sparkSession

    return spark


def change_type_column_contrato(table):
    decimal = ["HorasMantenimiento"]
    date = ["Vigencia"]

    table = cambiar_float(table, decimal)
    table = cambiar_timestamp(table, date)

    return table


def change_type_column_envase(table):
    return table


def change_type_column_equipo(table):
    return table


def change_type_column_equipos(table):
    return table


def change_type_column_evento(table):
    binary = ["TS"]

    return table


def change_type_column_evento_servicio_transporte(table):
    return table


def change_type_column_flota(table):
    return table


def change_type_column_mat_material(table):
    return table


def change_type_column_mat_materialconversionmedida(table):
    decimal = ["cantidad"]

    table = cambiar_float(table, decimal)

    return table


def change_type_column_orden_trabajo_arena_cantidad(table):
    decimal = ["cantidad_despacho", "CantidadSolicitada"]

    table = cambiar_float(table, decimal)

    return table


def change_type_column_orden_trabajo_transporte(table):
    return table


def change_type_column_pozo(table):
    binary = ["TS"]

    return table


def change_type_column_proveedor(table):
    binary = ["TS"]

    return table


def change_type_column_orden_trabajo_transporte_estado(table):
    return table


def change_type_column_servicio(table):
    binary = ["TS"]

    return table


def change_type_column_servicio_transporte(table):
    return table


def change_type_column_servicio_transporte_tipos_usos(table):
    return table


def change_type_column_servicio_transporte_categoria(table):
    binary = ["TS"]

    return table


def change_type_column_servicio_transporte_tipo_carga(table):
    return table


def change_type_column_solicitud_arena_malla(table):
    decimal = ["Cantidad"]

    table = cambiar_float(table, decimal)

    return table


def change_type_column_solicitud_transporte(table):
    return table


def change_type_column_solicitud_transporte_estado(table):
    return table


def change_type_column_solicitud_transporte_historial(table):
    return table


def change_type_column_solicitud_tipo_carga(table):
    return table


def change_type_column_solicitud_tipo_carga_vehiculo(table):
    return table


def change_type_column_tipos_usos(table):
    return table


def change_type_column_tsl_sala(table):
    return table


def change_type_column_t_cabhabilitas(table):
    date = ["fum"]

    table = cambiar_timestamp(table, date)

    return table


def change_type_column_t_dethabilitas(table):
    return table


def change_type_column_unidades(table):
    return table


def change_type_column_unidad_negocio(table):
    binary = ["TS"]

    return table


def change_type_column_usuario(table):
    binary = ["TS"]

    return table


def change_type_column_usuario_unidad_negocio(table):
    binary = ["TS"]

    return table


def change_type_column_vehiculo_flota(table):
    return table


def change_type_column_vehiculo_categoria(table):
    return table


def change_type_column_vehiculo_categoria_log(table):
    return table


def change_type_column_vehiculos(table):
    return table


def change_type_column_vehiculo_seguimiento(table):
    return table


def change_type_column_vehiculo_tarea(table):
    return table


def change_type_column_vehiculo_tipo(table):
    return table


def change_type_column_subzonas_interes(table):
    return table


def change_type_column_zonas_interes(table):
    return table


def change_type_column_zonas_interes_tipos(table):
    return table


def change_type_column_sitrack_report_detenido(table):
    return table


def cambiar_integer(TABLE, nombres):
    for i in range(len(nombres)):
        TABLE = TABLE.withColumn(nombres[i], TABLE[nombres[i]].cast(IntegerType()))
    return TABLE


def cambiar_string(TABLE, nombres):
    for i in range(len(nombres)):
        TABLE = TABLE.withColumn(nombres[i], TABLE[nombres[i]].cast(StringType()))
    return TABLE


def cambiar_byte(TABLE, nombres):
    for i in range(len(nombres)):
        TABLE = TABLE.withColumn(nombres[i], TABLE[nombres[i]].cast(ByteType()))
    return TABLE


def cambiar_timestamp(TABLE, nombres):
    for i in range(len(nombres)):
        TABLE = TABLE.withColumn(nombres[i], TABLE[nombres[i]].cast(TimestampType()))
    return TABLE


def cambiar_float(TABLE, nombres):
    for i in range(len(nombres)):
        TABLE = TABLE.withColumn(nombres[i], TABLE[nombres[i]].cast(FloatType()))
    return TABLE