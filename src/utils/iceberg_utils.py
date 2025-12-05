# src/utils/iceberg_utils.py

from pyspark.sql import SparkSession, DataFrame
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

class IcebergTable:
    """
    Helper para operaciones con tablas Iceberg
    """
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def create_or_replace(self, df: DataFrame, table_name: str, partition_by: str = None):
        """
        Crea o reemplaza tabla Iceberg
        """
        writer = df.writeTo(table_name).using("iceberg")
        
        if partition_by:
            # CAMBIO: No usar days() aquí, solo el nombre de columna
            writer = writer.partitionedBy(partition_by)
        
        writer.createOrReplace()
        logger.info(f"✅ Table '{table_name}' created/replaced successfully")
    
    def merge_data(self, df: DataFrame, table_name: str, merge_keys: list):
        """
        Hace MERGE en tabla Iceberg
        """
        df.createOrReplaceTempView("updates")
        
        merge_condition = " AND ".join([f"t.{key} = s.{key}" for key in merge_keys])
        
        merge_query = f"""
        MERGE INTO {table_name} t
        USING updates s
        ON {merge_condition}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
        
        self.spark.sql(merge_query)
        logger.info(f"✅ MERGE completed on '{table_name}'")
    
    def table_exists(self, table_name: str) -> bool:
        """
        Chequea si tabla existe
        """
        try:
            self.spark.table(table_name)
            return True
        except:
            return False