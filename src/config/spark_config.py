from pyspark.sql import SparkSession
import os
from pathlib import Path

class SparkConfig:
    """
    Configuración de Spark con soporte para Iceberg
    """
    
    @staticmethod
    def get_spark_session(app_name="DataPipeline"):
        """
        Crea sesión Spark con Iceberg configurado
        """
        
        # Paths
        project_root = Path(__file__).parent.parent.parent
        warehouse_path = project_root / "data" / "lakehouse"
        
        # Iceberg JAR path (ajustar según tu setup)
        iceberg_jar = str(project_root / "jars" / "iceberg-spark-runtime-3.5_2.12-1.4.3.jar")
        
        spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.jars", iceberg_jar) \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.local.type", "hadoop") \
            .config("spark.sql.catalog.local.warehouse", str(warehouse_path)) \
            .config("spark.sql.defaultCatalog", "local") \
            .config("spark.sql.catalogImplementation", "in-memory") \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g") \
            .config("spark.sql.shuffle.partitions", "4") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("ERROR")
        
        return spark
    
    @staticmethod
    def get_warehouse_path():
        """Retorna path del warehouse"""
        project_root = Path(__file__).parent.parent.parent
        return project_root / "data" / "lakehouse"