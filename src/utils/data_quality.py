from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when, isnan, isnull
from typing import Dict, List
import logging

logger = logging.getLogger(__name__)

class DataQualityChecker:
    """
    Validaciones de calidad de datos
    """
    
    @staticmethod
    def check_nulls(df: DataFrame, critical_columns: List[str]) -> Dict:
        """
        Chequea nulls en columnas críticas
        """
        results = {}
        total_rows = df.count()
        
        for col_name in critical_columns:
            null_count = df.filter(col(col_name).isNull()).count()
            null_percentage = (null_count / total_rows * 100) if total_rows > 0 else 0
            
            results[col_name] = {
                'null_count': null_count,
                'null_percentage': round(null_percentage, 2)
            }
            
            if null_count > 0:
                logger.warning(f"Column '{col_name}': {null_count} nulls ({null_percentage:.2f}%)")
        
        return results
    
    @staticmethod
    def check_duplicates(df: DataFrame, key_columns: List[str]) -> int:
        """
        Chequea duplicados basado en columnas clave
        """
        total_rows = df.count()
        distinct_rows = df.select(key_columns).distinct().count()
        duplicates = total_rows - distinct_rows
        
        if duplicates > 0:
            logger.warning(f"Found {duplicates} duplicate rows based on {key_columns}")
        
        return duplicates
    
    @staticmethod
    def validate_schema(df: DataFrame, expected_columns: List[str]) -> bool:
        """
        Valida que existan las columnas esperadas
        """
        actual_columns = set(df.columns)
        expected_columns_set = set(expected_columns)
        
        missing = expected_columns_set - actual_columns
        extra = actual_columns - expected_columns_set
        
        if missing:
            logger.error(f"Missing columns: {missing}")
            return False
        
        if extra:
            logger.warning(f"Extra columns found: {extra}")
        
        return True
    
    @staticmethod
    def summary_stats(df: DataFrame) -> None:
        """
        Imprime estadísticas básicas
        """
        logger.info(f"Total rows: {df.count()}")
        logger.info(f"Total columns: {len(df.columns)}")
        logger.info(f"Columns: {df.columns}")