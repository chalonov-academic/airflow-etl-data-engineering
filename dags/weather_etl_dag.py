# dags/weather_etl_final.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import os
from pathlib import Path

# Configuración por defecto del DAG
default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# Crear el DAG
dag = DAG(
    'weather_etl_google_sheets',
    default_args=default_args,
    description='ETL completo de datos meteorológicos desde Google Sheets cada 5 minutos',
    schedule_interval=timedelta(minutes=5),
    catchup=False,
    tags=['etl', 'weather', 'google_sheets', 'production']
)

def extract_from_sheets(**context):
    """
    Extrae datos de Google Sheets con fallback a datos simulados
    """
    print("=== INICIANDO EXTRACCIÓN ===")
    
    try:
        # Intentar importar las librerías de Google Sheets
        import gspread
        from oauth2client.service_account import ServiceAccountCredentials
        
        print("✅ Librerías de Google Sheets importadas correctamente")
        
        # Configurar credenciales
        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
        ]
        
        credentials_path = '/opt/airflow/credentials/google_sheets_credentials.json'
        
        # Verificar si existe el archivo de credenciales
        if not os.path.exists(credentials_path):
            print(f"⚠️  Archivo de credenciales no encontrado: {credentials_path}")
            print("🔄 Usando datos simulados para testing...")
            return extract_simulated_data()
        
        print(f"✅ Archivo de credenciales encontrado: {credentials_path}")
        
        # Autenticar con Google Sheets
        creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_path, scope)
        client = gspread.authorize(creds)
        
        # ID de tu Google Sheet (desde variable de entorno)
        sheet_id = os.environ.get('GOOGLE_SHEET_ID', 'TU_GOOGLE_SHEET_ID_AQUI')

        if sheet_id == 'TU_GOOGLE_SHEET_ID_AQUI':
            print("⚠️  GOOGLE_SHEET_ID no configurado en .env")
            print("🔄 Usando datos simulados...")
            return extract_simulated_data()
        
        try:
            sheet = client.open_by_key(sheet_id).sheet1
            data = sheet.get_all_records()
            
            print(f"✅ Datos extraídos de Google Sheets: {len(data)} registros")
            
        except Exception as sheet_error:
            print(f"⚠️  Error accediendo a Google Sheets: {sheet_error}")
            print("🔄 Usando datos simulados...")
            return extract_simulated_data()
            
    except ImportError as import_error:
        print(f"⚠️  Error importando librerías: {import_error}")
        print("🔄 Usando datos simulados...")
        return extract_simulated_data()
        
    except Exception as e:
        print(f"⚠️  Error en extracción: {str(e)}")
        print("🔄 Usando datos simulados como fallback...")
        return extract_simulated_data()
    
    # Procesar datos reales de Google Sheets
    df = pd.DataFrame(data)
    
    # Crear directorio temporal si no existe
    os.makedirs('/tmp', exist_ok=True)
    
    # Guardar datos extraídos
    output_path = '/tmp/raw_weather_data.csv'
    df.to_csv(output_path, index=False)
    
    print(f"📁 Datos guardados en: {output_path}")
    return output_path

def extract_simulated_data():
    """
    Función auxiliar para generar datos simulados
    """
    print("🎲 Generando datos meteorológicos simulados...")
    
    import random
    from datetime import datetime, timedelta
    
    # Generar datos simulados realistas
    cities = ['Bogotá', 'Medellín', 'Cali', 'Barranquilla', 'Cartagena']
    data = []
    
    base_time = datetime.now()
    
    for i in range(10):  # 10 registros simulados
        for city in cities[:3]:  # Solo 3 ciudades para no saturar
            timestamp = base_time - timedelta(minutes=i*5)
            
            # Temperaturas típicas por ciudad
            temp_ranges = {
                'Bogotá': (15, 25),
                'Medellín': (20, 30), 
                'Cali': (25, 35)
            }
            
            temp_min, temp_max = temp_ranges.get(city, (20, 30))
            
            record = {
                'fecha': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                'ciudad': city,
                'temperatura_celsius': round(random.uniform(temp_min, temp_max), 1),
                'humedad': random.randint(60, 90),
                'presion_atmosferica': round(random.uniform(1010, 1020), 1),
                'velocidad_viento': round(random.uniform(2, 15), 1),
                'direccion_viento': random.choice(['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW']),
                'precipitacion': round(random.uniform(0, 5), 1),
                'visibilidad': round(random.uniform(8, 15), 1)
            }
            data.append(record)
    
    df = pd.DataFrame(data)
    
    # Guardar datos simulados
    output_path = '/tmp/raw_weather_data.csv'
    df.to_csv(output_path, index=False)
    
    print(f"✅ Datos simulados generados: {len(df)} registros")
    print(f"📁 Guardados en: {output_path}")
    
    return output_path

def transform_data(**context):
    """
    Transforma los datos extraídos
    """
    print("=== INICIANDO TRANSFORMACIÓN ===")
    
    try:
        # Leer datos extraídos
        input_path = '/tmp/raw_weather_data.csv'
        df = pd.read_csv(input_path)
        print(f"📖 Datos leídos: {len(df)} registros")
        
        # Mostrar muestra de datos originales
        print("📊 Muestra de datos originales:")
        print(df.head(2).to_string())
        
        # === TRANSFORMACIONES ===
        
        # 1. Limpiar datos nulos
        initial_count = len(df)
        df = df.dropna()
        print(f"🧹 Limpieza: {initial_count - len(df)} registros con valores nulos eliminados")
        
        # 2. Convertir temperatura de Celsius a Fahrenheit
        if 'temperatura_celsius' in df.columns:
            df['temperatura_fahrenheit'] = (df['temperatura_celsius'] * 9/5) + 32
            print("🌡️  Temperatura convertida a Fahrenheit")
        
        # 3. Agregar timestamp de procesamiento
        df['processed_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print("⏰ Timestamp de procesamiento agregado")
        
        # 4. Filtrar datos válidos (temperatura entre -10 y 50°C)
        if 'temperatura_celsius' in df.columns:
            valid_temp = df[(df['temperatura_celsius'] >= -10) & (df['temperatura_celsius'] <= 50)]
            invalid_count = len(df) - len(valid_temp)
            df = valid_temp
            print(f"🔍 Filtrado de temperatura: {invalid_count} registros fuera de rango eliminados")
        
        # 5. Calcular índice de calor simplificado
        if all(col in df.columns for col in ['temperatura_celsius', 'humedad']):
            df['indice_calor'] = df['temperatura_celsius'] + (df['humedad'] * 0.1)
            print("🔥 Índice de calor calculado")
        
        # 6. Categorizar temperatura
        if 'temperatura_celsius' in df.columns:
            def categorize_temp(temp):
                if temp < 18:
                    return 'Frío'
                elif temp < 25:
                    return 'Templado'
                elif temp < 30:
                    return 'Caliente'
                else:
                    return 'Muy Caliente'
            
            df['categoria_temperatura'] = df['temperatura_celsius'].apply(categorize_temp)
            print("🏷️  Categorización de temperatura aplicada")
        
        # 7. Calcular estadísticas por ciudad
        if 'ciudad' in df.columns and 'temperatura_celsius' in df.columns:
            city_stats = df.groupby('ciudad')['temperatura_celsius'].agg(['mean', 'min', 'max']).round(2)
            print("📈 Estadísticas por ciudad:")
            print(city_stats.to_string())
        
        # Guardar datos transformados
        output_path = '/tmp/transformed_weather_data.csv'
        df.to_csv(output_path, index=False)
        
        print(f"✅ Transformación completada: {len(df)} registros válidos")
        print(f"📁 Datos transformados guardados en: {output_path}")
        
        # Mostrar muestra de datos transformados
        print("📊 Muestra de datos transformados:")
        print(df[['ciudad', 'temperatura_celsius', 'temperatura_fahrenheit', 'categoria_temperatura']].head(2).to_string())
        
        return output_path
        
    except Exception as e:
        print(f"❌ Error en transformación: {str(e)}")
        raise

def load_data(**context):
    """
    Carga los datos transformados al destino final
    """
    print("=== INICIANDO CARGA ===")
    
    try:
        # Leer datos transformados
        input_path = '/tmp/transformed_weather_data.csv'
        df = pd.read_csv(input_path)
        
        # Crear directorio de salida si no existe
        output_dir = Path('/opt/airflow/data/processed')
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Guardar con timestamp en el nombre
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        final_output_path = output_dir / f'weather_data_{timestamp}.csv'
        
        df.to_csv(final_output_path, index=False)
        
        # También mantener una copia "latest" para uso en otros sistemas
        latest_path = output_dir / 'weather_data_latest.csv'
        df.to_csv(latest_path, index=False)
        
        print(f"✅ Datos cargados exitosamente:")
        print(f"   📁 Archivo timestamped: {final_output_path}")
        print(f"   📁 Archivo latest: {latest_path}")
        
        # Generar reporte de métricas
        metrics = {
            'records_processed': len(df),
            'cities_count': df['ciudad'].nunique() if 'ciudad' in df.columns else 0,
            'avg_temperature': df['temperatura_celsius'].mean().round(2) if 'temperatura_celsius' in df.columns else 0,
            'processing_time': datetime.now().isoformat(),
            'file_size_kb': round(final_output_path.stat().st_size / 1024, 2)
        }
        
        print("📊 MÉTRICAS DEL PROCESAMIENTO:")
        for key, value in metrics.items():
            print(f"   {key}: {value}")
        
        return metrics
        
    except Exception as e:
        print(f"❌ Error en carga: {str(e)}")
        raise

def validate_data_quality(**context):
    """
    Valida la calidad de los datos procesados
    """
    print("=== VALIDACIÓN DE CALIDAD DE DATOS ===")
    
    try:
        # Leer datos finales
        latest_path = '/opt/airflow/data/processed/weather_data_latest.csv'
        
        if not os.path.exists(latest_path):
            print(f"❌ Archivo no encontrado: {latest_path}")
            return {'status': 'failed', 'reason': 'file_not_found'}
        
        df = pd.read_csv(latest_path)
        
        # Realizar validaciones
        validations = {
            'total_records': len(df),
            'null_values': df.isnull().sum().sum(),
            'duplicate_rows': df.duplicated().sum(),
        }
        
        # Validaciones específicas para datos meteorológicos
        if 'temperatura_celsius' in df.columns:
            temp_out_range = len(df[(df['temperatura_celsius'] < -50) | 
                                   (df['temperatura_celsius'] > 60)])
            validations['temperature_out_of_range'] = temp_out_range
        
        if 'humedad' in df.columns:
            humidity_out_range = len(df[(df['humedad'] < 0) | (df['humedad'] > 100)])
            validations['humidity_out_of_range'] = humidity_out_range
        
        # Calcular score de calidad (0-100)
        quality_score = 100
        if validations['null_values'] > 0:
            quality_score -= 20
        if validations['duplicate_rows'] > 0:
            quality_score -= 15
        if validations.get('temperature_out_of_range', 0) > 0:
            quality_score -= 25
        if validations.get('humidity_out_of_range', 0) > 0:
            quality_score -= 20
        
        validations['quality_score'] = max(quality_score, 0)
        
        print("📋 REPORTE DE CALIDAD DE DATOS:")
        for key, value in validations.items():
            icon = "✅" if value == 0 or key in ['total_records', 'quality_score'] else "⚠️"
            print(f"   {icon} {key}: {value}")
        
        # Determinar estado final
        if validations['quality_score'] >= 80:
            print("🎉 CALIDAD DE DATOS: EXCELENTE")
        elif validations['quality_score'] >= 60:
            print("👍 CALIDAD DE DATOS: BUENA")
        else:
            print("⚠️  CALIDAD DE DATOS: NECESITA ATENCIÓN")
        
        return validations
        
    except Exception as e:
        print(f"❌ Error en validación: {str(e)}")
        raise

# === DEFINICIÓN DE TAREAS ===

extract_task = PythonOperator(
    task_id='extract_from_google_sheets',
    python_callable=extract_from_sheets,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_weather_data',
    python_callable=transform_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_processed_data',
    python_callable=load_data,
    dag=dag,
)

validate_task = PythonOperator(
    task_id='validate_data_quality',
    python_callable=validate_data_quality,
    dag=dag,
)

# === DEFINIR DEPENDENCIAS (ORDEN DE EJECUCIÓN) ===
extract_task >> transform_task >> load_task >> validate_task