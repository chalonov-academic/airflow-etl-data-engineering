# Airflow ETL - Google Sheets Weather Data

Proyecto de ETL automatizado que extrae datos meteorológicos desde Google Sheets, los transforma y carga cada 5 minutos usando Apache Airflow.

## Descripción

Este sistema ETL automatizado:
- Extrae datos meteorológicos desde Google Sheets
- Transforma los datos (conversiones, cálculos, validaciones)
- Carga los datos procesados en archivos CSV
- Valida la calidad de los datos
- Se ejecuta automáticamente cada 5 minutos

## Arquitectura

```
Google Sheets → Airflow ETL → Archivos CSV Procesados
```

### Pipeline ETL
1. **Extract**: Conexión a Google Sheets API para obtener datos meteorológicos
2. **Transform**: Conversión de temperaturas, cálculo de índices, categorización, limpieza
3. **Load**: Guardado en archivos CSV con timestamp y archivo "latest"
4. **Validate**: Validación de calidad de datos y generación de métricas

## Estructura del Proyecto

```
airflow-etl-data-engineering/
├── docker-compose.yml          # Configuración de Docker
├── .env                        # Variables de entorno
├── dags/
│   └── weather_etl_dag.py      # DAG principal del ETL
├── credentials/
│   └── google_sheets_credentials.json  # Credenciales de Google Sheets
├── data/
│   └── processed/              # Archivos CSV procesados
├── logs/                       # Logs de Airflow
├── plugins/                    # Plugins personalizados
└── README.md
```

## Requisitos Previos

- Docker y Docker Compose instalados
- Cuenta de Google Cloud con APIs habilitadas:
  - Google Sheets API
  - Google Drive API
- Archivo de credenciales JSON de cuenta de servicio

## Configuración

### 1. Configurar Google Cloud

1. Crear proyecto en Google Cloud Console
2. Habilitar Google Sheets API y Google Drive API
3. Crear cuenta de servicio y descargar credenciales JSON
4. Colocar el archivo JSON en `credentials/google_sheets_credentials.json`

### 2. Preparar Google Sheets

1. Crear una hoja de Google Sheets
2. Agregar datos con estas columnas:
   ```
   fecha,ciudad,temperatura_celsius,humedad,presion_atmosferica,velocidad_viento,direccion_viento,precipitacion,visibilidad
   ```
3. Compartir la hoja con el email de la cuenta de servicio
4. Extraer el ID de la URL de Google Sheets

### 3. Configurar el proyecto

1. Clonar el repositorio
2. Crear archivo `.env`:
   ```bash
   AIRFLOW_UID=50000
   _AIRFLOW_WWW_USER_USERNAME=admin
   _AIRFLOW_WWW_USER_PASSWORD=admin123
   ```
3. Actualizar el Sheet ID en `dags/weather_etl_final.py`

## Instalación y Ejecución

### 1. Levantar los servicios

```bash
# Crear directorios necesarios
mkdir -p dags logs plugins data credentials

# Levantar Airflow
docker-compose up -d

# Verificar estado
docker-compose ps
```

### 2. Acceder a Airflow

- URL: http://localhost:8080
- Usuario: admin
- Contraseña: admin123

### 3. Activar el DAG

1. Buscar `weather_etl_google_sheets` en la interfaz
2. Activar el toggle
3. El DAG se ejecutará automáticamente cada 5 minutos

## Uso

### Ejecución Manual

En la interfaz de Airflow:
1. Seleccionar el DAG `weather_etl_google_sheets`
2. Hacer clic en "Trigger DAG"

### Monitorear Ejecuciones

```bash
# Ver logs del scheduler
docker-compose logs -f scheduler

# Ver logs del webserver
docker-compose logs -f webserver

# Ver archivos generados
docker-compose exec webserver ls -la /opt/airflow/data/processed/
```

### Ver Datos Procesados

```bash
# Ver último archivo procesado
docker-compose exec webserver head /opt/airflow/data/processed/weather_data_latest.csv
```

## Transformaciones Aplicadas

- **Conversión de temperatura**: Celsius a Fahrenheit
- **Índice de calor**: Cálculo simplificado basado en temperatura y humedad
- **Categorización**: Clasificación de temperaturas (Frío, Templado, Caliente, Muy Caliente)
- **Filtrado**: Eliminación de datos fuera de rangos válidos
- **Limpieza**: Eliminación de valores nulos y duplicados
- **Estadísticas**: Cálculos por ciudad (promedio, mínimo, máximo)

## Validaciones de Calidad

El sistema valida automáticamente:
- Valores nulos
- Filas duplicadas
- Temperaturas fuera de rango (-50°C a 60°C)
- Humedad fuera de rango (0% a 100%)
- Cálculo de score de calidad (0-100)

## Archivos de Salida

- `weather_data_YYYYMMDD_HHMMSS.csv`: Archivo con timestamp único
- `weather_data_latest.csv`: Última versión para consumo en tiempo real

## Troubleshooting

### DAG no aparece
- Verificar sintaxis del archivo Python
- Revisar logs del scheduler
- Reiniciar scheduler: `docker-compose restart scheduler`

### Error de conexión a Google Sheets
- Verificar que el archivo de credenciales existe
- Confirmar que la hoja está compartida con la cuenta de servicio
- Verificar que el Sheet ID sea correcto

### Contenedor no inicia
- Verificar recursos disponibles (mínimo 4GB RAM)
- Revisar logs: `docker-compose logs [servicio]`
- Limpiar volúmenes: `docker-compose down -v`

## Desarrollo

### Agregar nuevas transformaciones

Editar la función `transform_data()` en `dags/weather_etl_dag.py`:

```python
def transform_data(**context):
    # ... código existente ...
    
    # Nueva transformación
    df['nueva_columna'] = df['columna_existente'].apply(mi_funcion)
    
    # ... resto del código ...
```

### Cambiar frecuencia de ejecución

Modificar `schedule_interval` en el DAG:

```python
dag = DAG(
    'weather_etl_google_sheets',
    # ...
    schedule_interval=timedelta(minutes=10),  # Cambiar por la frecuencia deseada
    # ...
)
```

### Conectar otros destinos

Extender la función `load_data()` para enviar a:
- Bases de datos (PostgreSQL, MySQL)
- Data warehouses (BigQuery, Snowflake)
- APIs REST
- Sistemas de mensajería

## Comandos Útiles

```bash
# Parar servicios
docker-compose down

# Ver logs en tiempo real
docker-compose logs -f

# Acceder al contenedor
docker-compose exec scheduler bash

# Limpiar todo y reiniciar
docker-compose down -v
docker-compose up -d
```

## Tecnologías Utilizadas

- **Apache Airflow 2.7.1**: Orquestación de workflows
- **Docker**: Containerización
- **PostgreSQL**: Base de datos de Airflow
- **Python 3.9**: Lenguaje de programación
- **pandas**: Manipulación de datos
- **gspread**: Cliente de Google Sheets API

## Licencia

Este proyecto es de uso educativo y está disponible bajo licencia MIT.

## Contribuciones

Las contribuciones son bienvenidas. Por favor:
1. Fork el repositorio
2. Crear una rama para tu feature
3. Hacer commit de los cambios
4. Enviar pull request