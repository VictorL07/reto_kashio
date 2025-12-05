# src/jobs/bronze_to_silver_users.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, current_timestamp, trim
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent))

from config.spark_config import SparkConfig
from utils.logger import setup_logger
from utils.data_quality import DataQualityChecker
from utils.iceberg_utils import IcebergTable

logger = setup_logger(__name__)

def bronze_to_silver_users(spark: SparkSession):
    """
    Procesa usuarios: Bronze → Silver
    """
    
    logger.info("=" * 60)
    logger.info("Starting Bronze → Silver: Users")
    logger.info("=" * 60)
    
    bronze_path = str(SparkConfig.get_warehouse_path() / "bronze" / "users")
    logger.info(f"Reading from: {bronze_path}")
    
    df_raw = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(bronze_path)
    
    initial_count = df_raw.count()
    logger.info(f"📊 Initial records: {initial_count}")
    
    # Limpiar
    df_clean = df_raw \
        .filter(col("user_id").isNotNull()) \
        .withColumn("signup_date", to_date(col("signup_date"))) \
        .withColumn("device_type", trim(col("device_type"))) \
        .withColumn("country", trim(col("country"))) \
        .withColumn("processing_timestamp", current_timestamp()) \
        .distinct()
    
    # Seleccionar columnas finales
    df_final = df_clean.select(
        "user_id",
        "signup_date",
        "device_type",
        "country",
        "processing_timestamp"
    )
    
    final_count = df_final.count()
    duplicates_removed = initial_count - final_count
    
    logger.info(f"✅ Clean records: {final_count}")
    logger.info(f"🗑️  Duplicates removed: {duplicates_removed}")
    
    # Validar calidad
    dq = DataQualityChecker()
    dq.check_nulls(df_final, ["user_id", "signup_date", "device_type", "country"])
    
    # Escribir a Silver (Iceberg)
    table_name = "local.silver.users_dim"
    iceberg = IcebergTable(spark)
    
    if iceberg.table_exists(table_name):
        iceberg.merge_data(df_final, table_name, ["user_id"])
    else:
        iceberg.create_or_replace(df_final, table_name)
    
    logger.info("=" * 60)
    logger.info(f"✅ Bronze → Silver Users COMPLETED")
    logger.info(f"   Records processed: {final_count}")
    logger.info("=" * 60)
    
    return df_final

if __name__ == "__main__":
    spark = SparkConfig.get_spark_session("Bronze-Silver-Users")
    
    try:
        bronze_to_silver_users(spark)
    finally:
        spark.stop()