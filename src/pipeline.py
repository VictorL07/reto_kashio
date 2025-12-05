# src/pipeline.py

from config.spark_config import SparkConfig
from utils.logger import setup_logger
from jobs.bronze_to_silver_events import bronze_to_silver_events
from jobs.bronze_to_silver_transactions import bronze_to_silver_transactions
from jobs.bronze_to_silver_users import bronze_to_silver_users
from jobs.silver_to_gold import silver_to_gold
import time

logger = setup_logger(__name__)

def run_pipeline():
    """
    Ejecuta pipeline completo: Bronze → Silver → Gold
    """
    
    start_time = time.time()
    
    logger.info("🚀" * 30)
    logger.info("STARTING DATA PIPELINE")
    logger.info("🚀" * 30)
    
    spark = SparkConfig.get_spark_session("DataPipeline")
    
    try:
        # Stage 1: Bronze → Silver (Parallel en producción, secuencial para demo)
        logger.info("\n📦 STAGE 1: Bronze → Silver")
        logger.info("-" * 60)
        
        bronze_to_silver_events(spark)
        bronze_to_silver_transactions(spark)
        bronze_to_silver_users(spark)
        
        # Stage 2: Silver → Gold
        logger.info("\n✨ STAGE 2: Silver → Gold")
        logger.info("-" * 60)
        
        df_gold = silver_to_gold(spark)
        
        # Mostrar muestra de resultados
        logger.info("\n📊 Sample from Gold table:")
        df_gold.show(5, truncate=False)
        
        # Estadísticas finales
        elapsed_time = time.time() - start_time
        
        logger.info("\n" + "=" * 60)
        logger.info("✅ PIPELINE COMPLETED SUCCESSFULLY")
        logger.info(f"⏱️  Total execution time: {elapsed_time:.2f} seconds")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"❌ Pipeline failed: {str(e)}")
        raise
    
    finally:
        spark.stop()

if __name__ == "__main__":
    run_pipeline()