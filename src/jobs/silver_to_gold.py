# src/jobs/silver_to_gold.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, DecimalType
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent))

from config.spark_config import SparkConfig
from utils.logger import setup_logger
from utils.iceberg_utils import IcebergTable

logger = setup_logger(__name__)

def silver_to_gold(spark: SparkSession):
    """
    Crea tabla Gold: user_session_analysis
    """
    
    logger.info("=" * 60)
    logger.info("Starting Silver → Gold: user_session_analysis")
    logger.info("=" * 60)
    
    # Leer tablas Silver
    logger.info("📖 Reading Silver tables...")
    
    df_events = spark.table("local.silver.events_clean")
    df_transactions = spark.table("local.silver.transactions_clean")
    df_users = spark.table("local.silver.users_dim")
    
    logger.info(f"   Events: {df_events.count()} records")
    logger.info(f"   Transactions: {df_transactions.count()} records")
    logger.info(f"   Users: {df_users.count()} records")
    
    # Agregar eventos por sesión
    logger.info("🔄 Aggregating events by session...")
    
    df_session_events = df_events.groupBy("session_id", "user_id").agg(
        min("event_timestamp").alias("session_start_time"),
        max("event_timestamp").alias("session_end_time"),
        count("event_id").alias("total_events"),
        collect_list("event_type").alias("event_types"),
        first("event_type").alias("first_event_type"),
        last("event_type").alias("last_event_type")
    ).withColumn(
        "session_duration_seconds",
        (unix_timestamp(col("session_end_time")) - unix_timestamp(col("session_start_time"))).cast(IntegerType())
    ).withColumn(
        "session_date",
        to_date(col("session_start_time"))
    )
    
    # JOIN con usuarios (drop processing_timestamp)
    logger.info("🔗 Joining with users...")
    df_users_clean = df_users.drop("processing_timestamp")
    df_with_users = df_session_events.join(df_users_clean, "user_id", "left")
    
    # JOIN con transacciones (drop processing_timestamp y castear amount a DECIMAL)
    logger.info("🔗 Joining with transactions...")
    df_transactions_clean = df_transactions \
        .drop("processing_timestamp") \
        .withColumn("amount", col("amount").cast(DecimalType(10, 2))) \
        .withColumnRenamed("amount", "transaction_amount") \
        .withColumnRenamed("currency", "transaction_currency")
    
    df_gold = df_with_users.join(
        df_transactions_clean,
        ["session_id", "user_id"],
        "left"
    )
    
    # Seleccionar y transformar columnas finales con tipos correctos
    df_gold_final = df_gold.select(
        # Identificadores
        col("user_id"),
        col("session_id"),
        
        # Datos del usuario
        col("signup_date"),
        col("device_type"),
        col("country"),
        
        # Métricas de sesión
        col("session_start_time"),
        col("session_end_time"),
        col("session_duration_seconds"),  # Ya es INT
        col("session_date"),
        
        # Agregados de eventos
        col("total_events").cast(IntegerType()),  # Forzar INT en lugar de BIGINT
        col("event_types"),
        col("first_event_type"),
        col("last_event_type"),
        
        # Datos de transacción
        col("transaction_id"),
        col("transaction_amount"),  # Ya es DECIMAL(10,2)
        col("transaction_currency"),
        col("transaction_timestamp"),
        
        # Flags calculados
        when(col("transaction_id").isNotNull(), lit(True)).otherwise(lit(False)).alias("has_transaction"),
        when(col("transaction_timestamp").isNotNull(),
             (unix_timestamp(col("transaction_timestamp")) - unix_timestamp(col("session_start_time"))).cast(IntegerType())
        ).alias("time_to_purchase_seconds"),  # Forzar INT
        
        # Metadata
        current_timestamp().alias("processing_timestamp"),
        lit(1).alias("data_version"),
        lit("local_pipeline").alias("source_system")
    )
    
    final_count = df_gold_final.count()
    purchase_count = df_gold_final.filter(col("has_transaction") == True).count()
    conversion_rate = (purchase_count / final_count * 100) if final_count > 0 else 0
    
    logger.info(f"📊 Gold table metrics:")
    logger.info(f"   Total sessions: {final_count}")
    logger.info(f"   Sessions with purchase: {purchase_count}")
    logger.info(f"   Conversion rate: {conversion_rate:.2f}%")
    
    # Mostrar schema para verificar
    logger.info("\n📋 Schema validation:")
    df_gold_final.printSchema()
    
    # Escribir a Gold (Iceberg)
    table_name = "local.gold.user_session_analysis"
    iceberg = IcebergTable(spark)
    
    if iceberg.table_exists(table_name):
        iceberg.merge_data(df_gold_final, table_name, ["session_id", "user_id"])
    else:
        iceberg.create_or_replace(df_gold_final, table_name, "session_date")
    
    logger.info("=" * 60)
    logger.info(f"✅ Silver → Gold COMPLETED")
    logger.info(f"   Records in Gold: {final_count}")
    logger.info("=" * 60)
    
    return df_gold_final

if __name__ == "__main__":
    spark = SparkConfig.get_spark_session("Silver-Gold")
    
    try:
        silver_to_gold(spark)
    finally:
        spark.stop()