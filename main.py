from dependencies.config import Config
from jobs.etl_jobs import EtlJobs
import dependencies.spark as spark
import dependencies.logging as log
import utils.utils as util
from pyspark.sql import SparkSession
from pyspark import SparkConf

if __name__ == '__main__':

    # Read file Config.ini
    read_config = Config('configs/config.ini')

    # Initialize Spark
    spark = spark.get_spark_session(read_config)

    list_tables = util.stringToList(read_config.get_list_tables())

    for table in list_tables:
        # Instanciate etl_jobs
        etl_jobs = EtlJobs(table, read_config)

        # Extract Data
        data_extract = etl_jobs.extract_data(spark)

        # Transformar data si es necesario
        data_transformed = etl_jobs.transform_data(data_extract)

        # Load Data
        etl_jobs.load_data(data_transformed)

    # Detenemos Spark
    spark.stop()