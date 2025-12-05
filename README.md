# PARTE 1: Arquitectura

## 1. Selección de Componentes

### Ingesta de Datos

**Eventos (Streaming)**
- Kinesis Data Streams con Firehose porque necesitamos capturar eventos en tiempo real y además mantener un buffer para reprocesamiento. La retención de 7 días nos da margen para corregir errores en la lógica de transformación sin perder datos.
- Firehose convierte automáticamente de JSON a Parquet y particiona por fecha, lo que nos ahorra escribir código de conversión y reduce significativamente el costo de storage en S3 (compresión snappy).

**Transacciones (Batch)**
- EventBridge + Lambda porque los archivos CSV llegan cada hora desde un SFTP externo. Con Lambda solo pagamos por las 720 ejecuciones mensuales (~$2), mientras que un servidor SFTP administrado (Transfer Family) nos costaría $220/mes constantes.
- La Lambda descarga el archivo, hace validaciones básicas (schema, nulls) y sube a S3. Simple y efectivo.

**Usuarios (Dimensional)**
- Lambda con extracción incremental diaria. La tabla de usuarios no cambia mucho (quizás un 2-3% diario con nuevos registros), así que no tiene sentido pagar $145/mes por DMS cuando Lambda nos cuesta $3/mes.
- Usamos watermarking en DynamoDB para trackear el último `updated_at` procesado y solo extraer deltas. La Lambda está en la VPC para conectarse a RDS de forma segura.

### Storage

**S3 + Iceberg**

La decisión de usar Iceberg sobre Parquet plano viene de experiencia previa lidiando con datos que llegan tarde. Con Parquet tendrías que:
- Leer toda la partición
- Hacer merge en memoria
- Reescribir todo el archivo

Con Iceberg hacemos un `MERGE` SQL y el engine se encarga de la complejidad. Además nos da snapshots para rollback si metemos la pata en alguna transformación.

El particionamiento por `days(session_date)` es hidden, así que los analistas no tienen que acordarse de incluir la partición en sus queries. Iceberg lo optimiza automáticamente.

### Procesamiento

**Glue ETL**

Necesitábamos Spark para procesar los datos y Glue nos evita gestionar un cluster EMR. Los jobs se facturan por DPU-hora, y con las transformaciones que tenemos (joins, agregaciones, deduplicación) estamos gastando ~40 DPU-horas al mes ($100).

Evaluamos usar Lambda para todo, pero Lambda tiene límite de 15 minutos y memoria de 10GB. Para procesar millones de eventos y hacer joins complejos, Spark es más apropiado.

**Step Functions**

Para orquestar los jobs usamos Step Functions porque nuestro pipeline es bastante lineal:
1. Tres jobs Bronze→Silver en paralelo
2. Un job Silver→Gold que depende de los tres anteriores
3. Notificación

No necesitamos todas las features de Airflow (pools, custom operators, SLAs complejos). Step Functions nos cuesta literalmente centavos y se integra nativamente con Glue usando `.sync` para esperar a que termine cada job.

Si en el futuro el pipeline se complica mucho, podemos migrar a Airflow self-hosted en EC2 (~$50/mes) o evaluar MWAA si justifica el costo.

### Red

**VPC + Endpoint S3**

La Lambda que extrae de RDS tiene que estar en la VPC porque RDS está en subnets privadas (security best practice). El tema es que Lambda en VPC por default no puede acceder a internet, entonces para subir a S3 tienes dos opciones:
- NAT Gateway: $32/mes + $0.045/GB
- VPC Endpoint: $7/mes + $0.01/GB

Elegimos VPC Endpoint porque con ~100GB/mes de uploads sale mucho más barato.

El trade-off es que Lambda en VPC tiene cold start más lento (10-15 seg vs 1 seg), pero como nuestros jobs son diarios/hourly, no es crítico.

## 2. Esquema de la Tabla Gold
```sql
CREATE TABLE gold.user_session_analysis (
    user_id STRING,
    session_id STRING,
    
    -- Datos del usuario (snapshot al momento de la sesión)
    signup_date DATE,
    device_type STRING,
    country STRING,
    
    -- Métricas de la sesión
    session_start_time TIMESTAMP,
    session_end_time TIMESTAMP,
    session_duration_seconds INT,
    session_date DATE,  -- Para particionar
    
    -- Agregados de eventos
    total_events INT,
    event_types ARRAY<STRING>,
    first_event_type STRING,
    last_event_type STRING,
    
    -- Datos de transacción (nullable porque no todas las sesiones compran)
    transaction_id STRING,
    transaction_amount DECIMAL(10,2),
    transaction_currency STRING,
    transaction_timestamp TIMESTAMP,
    
    -- Flags calculados
    has_transaction BOOLEAN,
    time_to_purchase_seconds INT,
    
    -- Metadata
    processing_timestamp TIMESTAMP,
    data_version INT,
    source_system STRING
)
USING iceberg
PARTITIONED BY (days(session_date))
TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'snappy',
    'format-version' = '2',
    'write.merge.mode' = 'merge-on-read'
)
LOCATION 's3://digital-services-datalake/gold/user_session_analysis/';
```

**Decisiones de diseño:**

- `ARRAY<STRING>` para `event_types` porque queremos mantener el orden temporal de los eventos en la sesión. Es más flexible para análisis de funnel que tener columnas separadas.

- Los campos de transacción son nullable con LEFT JOIN desde transactions. Preferimos esto a tener dos tablas separadas (sessions con compra vs sin compra) porque simplifica las queries analíticas.

- `session_date` es derivado de `session_start_time` y se usa para particionar. Iceberg lo hace transparente - el usuario no tiene que saber que está particionado.

- `DECIMAL(10,2)` para amounts porque floats dan problemas de precisión con dinero. Aprendimos esto de la forma difícil.

- `data_version` por si necesitamos cambiar la lógica de agregación en el futuro y queremos saber qué registros usan qué versión.

## 3. Manejo de Escenarios

### Late-arriving data

El problema ocurre cuando un evento se genera a las 10 AM pero por problemas de red llega a Kinesis a las 4 PM. Para ese momento ya corrimos el pipeline de la mañana y la sesión está incompleta en la tabla gold.

**Solución práctica:**

1. Kinesis retiene 7 días. Si detectamos el problema, podemos reprocesar desde un timestamp específico usando `GetShardIterator` con `AT_TIMESTAMP`.

2. El job Silver→Gold hace `MERGE` en lugar de `INSERT`:
```sql
MERGE INTO gold.user_session_analysis t
USING new_sessions s
ON t.session_id = s.session_id AND t.user_id = s.user_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

Esto es idempotente. Si una sesión ya existe, la actualizamos con los datos más completos. Si es nueva, la insertamos.

3. Corremos el pipeline con una ventana de lookback. Por ejemplo, cada vez que procesamos, miramos los últimos 2 días de datos en Bronze, no solo el día actual. Esto captura eventos que llegaron tarde.

El costo de reprocesar es bajo porque Iceberg solo reescribe las particiones afectadas, no toda la tabla.

### Calidad de datos

**Duplicados:**

En cada capa tenemos deduplicación:

Bronze→Silver:
```python
window = Window.partitionBy("event_id").orderBy(col("event_timestamp").desc())
df_clean = df.withColumn("rn", row_number().over(window)) \
             .filter(col("rn") == 1) \
             .drop("rn")
```

Esto se basa en que `event_id` es único. Si llega el mismo evento dos veces (retry del producer), nos quedamos con el más reciente.

Silver→Gold:

El `MERGE` deduplica automáticamente por `(session_id, user_id)`.

**Datos corruptos:**

Cuando leemos JSON con Spark usamos modo PERMISSIVE con `columnNameOfCorruptRecord`. Los registros que no parsean se marcan y los mandamos a S3 quarantine:
```python
df_valid = df.filter(col("_corrupt_record").isNull())
df_corrupt = df.filter(col("_corrupt_record").isNotNull())

df_corrupt.write.json("s3://.../quarantine/events/")
```

Publicamos una métrica a CloudWatch con el count de registros corruptos. Si pasa de un threshold, se dispara una alarma a Slack.

Para rollback usamos Iceberg snapshots:
```sql
-- Ver historial
SELECT * FROM gold.user_session_analysis.snapshots;

-- Rollback si metimos datos malos
CALL system.rollback_to_snapshot('gold.user_session_analysis', 12345);
```

Esto no mueve datos, solo cambia el metadata pointer. Es instantáneo.

**Validaciones:**

Usamos Glue Data Quality rules antes de escribir a Silver:
- `IsUnique "event_id"`
- `IsComplete "user_id"`
- `ColumnValues "event_type" in ["page_view", "click", "purchase", ...]`
- `ColumnValues "session_duration_seconds" >= 0`

Si las reglas fallan, el job falla y se dispara la alerta. Preferimos fallar rápido que propagar datos malos.

---

**Costos estimados:**
- Ingesta: ~$110/mes (Kinesis + Firehose + Lambdas)
- Storage: ~$15/mes (S3 con compresión)
- Compute: ~$100/mes (Glue ETL)
- Red: ~$15/mes (VPC Endpoint)
- Misc: ~$10/mes (CloudWatch, Secrets Manager, DynamoDB)

**Total: ~$250/mes** para una plataforma de datos completa. Escalable y serverless.

# Mock Data Generator - Digital Services Inc.

Generador de datos de prueba para el reto técnico de Data Engineering.

## 📋 Descripción

Este script genera datos simulados para tres fuentes:

1. **Usuarios (Dimensional)** - `users.csv`
   - user_id, signup_date, device_type, country

2. **Eventos de App (Streaming)** - `events.jsonl`
   - event_id, session_id, user_id, event_type, event_timestamp, event_details

3. **Transacciones (Batch)** - `transactions.csv`
   - transaction_id, session_id, user_id, amount, currency, transaction_timestamp

## 🚀 Instalación y Uso

### Instalación

```bash
# Crear entorno virtual (recomendado)
python -m venv venv
source venv/bin/activate  # En Windows: venv\Scripts\activate

# Instalar dependencias
pip install -r requirements.txt
```

### Ejecución

```bash
# Generar datos con configuración por defecto
python generate_mock_data.py
```

Los archivos se generarán en el directorio `data/`:
```
data/
├── users.csv
├── events.jsonl
└── transactions.csv
```

## ⚙️ Configuración

Puedes modificar los parámetros en `config.ini`:

```ini
[data_volumes]
num_users = 100          # Número de usuarios a generar
num_sessions = 500       # Número de sesiones
num_events = 2000        # Número de eventos
num_transactions = 300   # Número de transacciones

[data_quality]
late_arrival_rate = 0.05  # 5% de eventos con retraso
duplicate_rate = 0.02     # 2% de transacciones duplicadas
```

## 📊 Características de los Datos

### Datos de Calidad Simulados

El generador incluye escenarios realistas para probar tu pipeline:

- **Late-arriving data**: 5% de eventos con timestamps retrasados (2-48 horas)
- **Duplicados**: 2% de transacciones duplicadas (mismo transaction_id)
- **Datos relacionados**: Eventos y transacciones vinculados a sesiones y usuarios válidos

### Estructura de Datos

#### Users (CSV)
```csv
user_id,signup_date,device_type,country
USR_00001,2024-03-15 14:23:00,iOS,PE
USR_00002,2024-05-20 09:15:00,Android,US
```

#### Events (JSONL)
```json
{"event_id": "EVT_00000001", "session_id": "SES_000001", "user_id": "USR_00001", "event_type": "page_view", "event_timestamp": "2024-11-15T14:30:00", "event_details": {"page_url": "/page/12", "referrer": "google"}}
```

#### Transactions (CSV)
```csv
transaction_id,session_id,user_id,amount,currency,transaction_timestamp
TXN_0000001,SES_000123,USR_00045,1250.50,USD,2024-11-15 14:45:00
```

## 🔍 Validación de Datos

Para verificar que los datos se generaron correctamente:

```python
import pandas as pd
import json

# Verificar usuarios
users = pd.read_csv('data/users.csv')
print(f"Total usuarios: {len(users)}")
print(f"Países únicos: {users['country'].nunique()}")

# Verificar eventos
with open('data/events.jsonl', 'r') as f:
    events = [json.loads(line) for line in f]
print(f"Total eventos: {len(events)}")

# Verificar transacciones
txns = pd.read_csv('data/transactions.csv')
print(f"Total transacciones: {len(txns)}")
print(f"Duplicados detectados: {txns.duplicated(subset=['transaction_id']).sum()}")
```

