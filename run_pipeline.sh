#!/bin/bash
# run_pipeline.sh

echo "🔧 Setting up project..."

# Crear directorio jars
mkdir -p jars

# Descargar Iceberg JAR
echo "📦 Downloading Iceberg JAR..."
if command -v curl &> /dev/null; then
    curl -L -o jars/iceberg-spark-runtime-3.5_2.12-1.4.3.jar \
        https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.4.3/iceberg-spark-runtime-3.5_2.12-1.4.3.jar
elif command -v wget &> /dev/null; then
    wget -P jars/ \
        https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.4.3/iceberg-spark-runtime-3.5_2.12-1.4.3.jar
else
    echo "❌ Error: Neither curl nor wget is available"
    echo "Please download manually from:"
    echo "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.4.3/"
    exit 1
fi

# Verificar descarga
if [ -f "jars/iceberg-spark-runtime-3.5_2.12-1.4.3.jar" ]; then
    echo "✅ Iceberg JAR downloaded successfully"
    ls -lh jars/iceberg-spark-runtime-3.5_2.12-1.4.3.jar
else
    echo "❌ Failed to download JAR"
    exit 1
fi

echo "✅ Setup complete!"

echo "========================================="
echo "  Data Pipeline Execution"
echo "========================================="
echo ""

# Activar virtual environment si existe
if [ -d "venv" ]; then
    source venv/bin/activate
fi

# Copiar datos generados a Bronze
echo "📦 Copying generated data to Bronze layer..."
mkdir -p data/lakehouse/bronze/events
mkdir -p data/lakehouse/bronze/transactions
mkdir -p data/lakehouse/bronze/users

cp data/generator/data_test/events.json data/lakehouse/bronze/events/
cp data/generator/data_test/transactions.csv data/lakehouse/bronze/transactions/
cp data/generator/data_test/users.csv data/lakehouse/bronze/users/

echo "✅ Data copied successfully"
echo ""

# Ejecutar pipeline
echo "🚀 Starting pipeline execution..."
python src/pipeline.py

echo ""
echo "========================================="
echo "  Pipeline execution finished"
echo "========================================="