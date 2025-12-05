# src/jobs/bronze_to_silver_transactions.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, row_number, current_timestamp, trim
from pyspark.sql.window import Window
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent))

from config.spark_config import SparkConfig
from utils.logger import setup_logger
from utils.data_quality import DataQualityChecker
from utils.iceberg_utils import IcebergTable

logger = setup_logger(__name__)

def bronze_to_silver_transactions(spark: SparkSession):
    """
    Procesa transacciones: Bronze → Silver
    """
    
    logger.info("=" * 60)
    logger.info("Starting Bronze → Silver: Transactions")
    logger.info("=" * 60)
    
    bronze_path = str(SparkConfig.get_warehouse_path() / "bronze" / "transactions")
    logger.info(f"Reading from: {bronze_path}")
    
    df_raw = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(bronze_path)
    
    initial_count = df_raw.count()
    logger.info(f"📊 Initial records: {initial_count}")
    
    # Limpiar
    df_clean = df_raw \
        .filter(col("transaction_id").isNotNull()) \
        .filter(col("session_id").isNotNull()) \
        .filter(col("user_id").isNotNull()) \
        .filter(col("amount").isNotNull()) \
        .filter(col("amount") > 0) \
        .withColumn("transaction_timestamp", to_timestamp(col("transaction_timestamp"))) \
        .withColumn("currency", trim(col("currency"))) \
        .withColumn("processing_timestamp", current_timestamp())
    
    # Deduplicar por transaction_id
    window_dedup = Window.partitionBy("transaction_id").orderBy(col("transaction_timestamp").desc())
    
    df_deduped = df_clean \
        .withColumn("rn", row_number().over(window_dedup)) \
        .filter(col("rn") == 1) \
        .drop("rn")
    
    # Seleccionar columnas finales
    df_final = df_deduped.select(
        "transaction_id",
        "session_id",
        "user_id",
        "amount",
        "currency",
        "transaction_timestamp",
        "processing_timestamp"
    )
    
    final_count = df_final.count()
    duplicates_removed = df_clean.count() - final_count
    
    logger.info(f"✅ Clean records: {final_count}")
    logger.info(f"🗑️  Duplicates removed: {duplicates_removed}")
    
    # Validar calidad
    dq = DataQualityChecker()
    dq.check_nulls(df_final, ["transaction_id", "session_id", "user_id", "amount"])
    
    # Escribir a Silver (Iceberg)
    table_name = "local.silver.transactions_clean"
    iceberg = IcebergTable(spark)
    
    if iceberg.table_exists(table_name):
        iceberg.merge_data(df_final, table_name, ["transaction_id"])
    else:
        iceberg.create_or_replace(df_final, table_name)
    
    logger.info("=" * 60)
    logger.info(f"✅ Bronze → Silver Transactions COMPLETED")
    logger.info(f"   Records processed: {final_count}")
    logger.info("=" * 60)
    
    return df_final

if __name__ == "__main__":
    spark = SparkConfig.get_spark_session("Bronze-Silver-Transactions")
    
    try:
        bronze_to_silver_transactions(spark)
    finally:
        spark.stop()