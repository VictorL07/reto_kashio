# src/jobs/bronze_to_silver_events.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, row_number, current_timestamp
from pyspark.sql.window import Window
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent))

from config.spark_config import SparkConfig
from utils.logger import setup_logger
from utils.data_quality import DataQualityChecker
from utils.iceberg_utils import IcebergTable

logger = setup_logger(__name__)

def bronze_to_silver_events(spark: SparkSession):
    """
    Procesa eventos: Bronze → Silver
    """
    
    logger.info("=" * 60)
    logger.info("Starting Bronze → Silver: Events")
    logger.info("=" * 60)
    
    bronze_path = str(SparkConfig.get_warehouse_path() / "bronze" / "events")
    logger.info(f"Reading from: {bronze_path}")
    
    df_raw = spark.read \
        .option("mode", "PERMISSIVE") \
        .option("columnNameOfCorruptRecord", "_corrupt_record") \
        .json(bronze_path)
    
    initial_count = df_raw.count()
    logger.info(f"📊 Initial records: {initial_count}")
    
    # Filtrar corruptos si existe la columna
    if "_corrupt_record" in df_raw.columns:
        df_valid = df_raw.filter(col("_corrupt_record").isNull())
        corrupt_count = initial_count - df_valid.count()
        if corrupt_count > 0:
            logger.warning(f"⚠️  Corrupt records: {corrupt_count}")
    else:
        df_valid = df_raw
        corrupt_count = 0
        logger.info("✅ No corrupt records found")
    
    # Validar schema
    expected_columns = ["event_id", "session_id", "user_id", "event_type", "event_timestamp", "event_details"]
    dq = DataQualityChecker()
    
    if not dq.validate_schema(df_valid, expected_columns):
        raise ValueError("Schema validation failed")
    
    # Limpiar
    df_clean = df_valid \
        .filter(col("event_id").isNotNull()) \
        .filter(col("user_id").isNotNull()) \
        .filter(col("session_id").isNotNull()) \
        .filter(col("event_type").isNotNull()) \
        .withColumn("event_timestamp", to_timestamp(col("event_timestamp"))) \
        .withColumn("processing_timestamp", current_timestamp())
    
    # Deduplicar por event_id
    window_dedup = Window.partitionBy("event_id").orderBy(col("event_timestamp").desc())
    
    df_deduped = df_clean \
        .withColumn("rn", row_number().over(window_dedup)) \
        .filter(col("rn") == 1) \
        .drop("rn")
    
    # Eliminar _corrupt_record si existe
    if "_corrupt_record" in df_deduped.columns:
        df_deduped = df_deduped.drop("_corrupt_record")
    
    # Seleccionar columnas finales
    df_final = df_deduped.select(
        "event_id",
        "session_id",
        "user_id",
        "event_type",
        "event_timestamp",
        "event_details",
        "processing_timestamp"
    )
    
    final_count = df_final.count()
    duplicates_removed = df_clean.count() - final_count
    
    logger.info(f"✅ Clean records: {final_count}")
    logger.info(f"🗑️  Duplicates removed: {duplicates_removed}")
    
    # Validar calidad
    dq.check_nulls(df_final, ["event_id", "user_id", "session_id"])
    
    # Escribir a Silver (Iceberg)
    table_name = "local.silver.events_clean"
    iceberg = IcebergTable(spark)
    
    if iceberg.table_exists(table_name):
        iceberg.merge_data(df_final, table_name, ["event_id"])
    else:
        iceberg.create_or_replace(df_final, table_name)
    
    logger.info("=" * 60)
    logger.info(f"✅ Bronze → Silver Events COMPLETED")
    logger.info(f"   Records processed: {final_count}")
    logger.info("=" * 60)
    
    return df_final

if __name__ == "__main__":
    spark = SparkConfig.get_spark_session("Bronze-Silver-Events")
    
    try:
        bronze_to_silver_events(spark)
    finally:
        spark.stop()