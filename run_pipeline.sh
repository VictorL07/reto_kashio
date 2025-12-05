#!/bin/bash
# run_pipeline.sh

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