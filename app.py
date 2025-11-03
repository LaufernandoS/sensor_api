from fastapi import FastAPI, HTTPException, Query
from contextlib import asynccontextmanager
from typing import Optional
import csv
from pathlib import Path
from datetime import datetime

from sensors import Sensor, SensorManager
from models.sensor import SensorReading, SensorInfo, SensorStatus
from etl.process import ETLPipeline
from analytics.stats import SensorAnalytics

# Gerenciador global de sensores
manager = SensorManager()

# Pipeline ETL
etl = ETLPipeline()

# Analytics
analytics = SensorAnalytics()

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Gerencia ciclo de vida da aplicação (startup/shutdown)."""
    # STARTUP: Iniciar sensores
    print("\n🚀 Iniciando sensores...")
    manager.add_sensor(Sensor("TEMP-001", "temperature", interval=2.0))
    manager.add_sensor(Sensor("HUM-001", "humidity", interval=2.5))
    manager.add_sensor(Sensor("NOISE-001", "noise", interval=3.0))
    print(f"✓ {manager.get_active_count()} sensores ativos\n")
    
    yield  # API roda aqui
    
    # SHUTDOWN: Parar sensores
    print("\n🛑 Encerrando sensores...")
    manager.shutdown_all()
    print("✓ Aplicação finalizada\n")

app = FastAPI(
    title="Sensor Monitoring API",
    description="API para monitoramento de sensores IoT com análise em tempo real",
    version="1.0.0",
    lifespan=lifespan
)

# ============================================================================
# ROTAS - INFORMAÇÕES DOS SENSORES
# ============================================================================

@app.get("/", tags=["Info"])
async def root():
    """Informações básicas da API."""
    return {
        "message": "Sensor Monitoring API",
        "docs": "/docs",
        "endpoints": {
            "sensors": "/sensors",
            "readings": "/readings",
            "health": "/health"
        }
    }

@app.get("/health", tags=["Info"])
async def health_check():
    """Verifica status da API e sensores."""
    active_count = manager.get_active_count()
    return {
        "status": "healthy" if active_count > 0 else "degraded",
        "active_sensors": active_count,
        "timestamp": datetime.now().isoformat()
    }

@app.get("/sensors", response_model=list[SensorInfo], tags=["Sensors"])
async def list_sensors():
    """Lista todos os sensores registrados."""
    sensors = manager.list_sensors()
    
    return [
        SensorInfo(
            sensor_id=s.sensor_id,
            sensor_type=s.sensor_type,
            status="paused" if not s._pause_event.is_set() else "running",
            interval=s.interval
        )
        for s in sensors
    ]

@app.get("/sensors/{sensor_id}", response_model=SensorInfo, tags=["Sensors"])
async def get_sensor(sensor_id: str):
    """Retorna informações de um sensor específico."""
    sensor = manager.get_sensor(sensor_id)
    
    if not sensor:
        raise HTTPException(status_code=404, detail=f"Sensor {sensor_id} não encontrado")
    
    return SensorInfo(
        sensor_id=sensor.sensor_id,
        sensor_type=sensor.sensor_type,
        status="paused" if not sensor._pause_event.is_set() else "running",
        interval=sensor.interval
    )

# ============================================================================
# ROTAS - LEITURAS DOS SENSORES
# ============================================================================

@app.get("/readings", response_model=list[SensorReading], tags=["Readings"])
async def get_readings(
    sensor_id: Optional[str] = Query(None, description="Filtrar por ID do sensor"),
    limit: int = Query(50, ge=1, le=1000, description="Número máximo de leituras"),
    sensor_type: Optional[str] = Query(None, description="Filtrar por tipo (temperature, humidity, noise)")
):
    """
    Retorna leituras dos sensores.
    
    - **sensor_id**: Filtrar por sensor específico
    - **sensor_type**: Filtrar por tipo de sensor
    - **limit**: Quantidade de leituras (padrão: 50, máx: 1000)
    """
    csv_path = Path("data/raw_data.csv")
    
    if not csv_path.exists():
        return []
    
    readings = []
    
    with open(csv_path, mode='r') as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            # Aplicar filtros
            if sensor_id and row['sensor_id'] != sensor_id:
                continue
            if sensor_type and row['sensor_type'] != sensor_type:
                continue
            
            readings.append(SensorReading(
                timestamp=row['timestamp'],
                sensor_id=row['sensor_id'],
                sensor_type=row['sensor_type'],
                value=float(row['value']),
                unit=row['unit']
            ))
    
    # Retornar as últimas N leituras
    return readings[-limit:]

@app.get("/readings/latest", response_model=list[SensorReading], tags=["Readings"])
async def get_latest_readings():
    """Retorna a última leitura de cada sensor."""
    csv_path = Path("data/raw_data.csv")
    
    if not csv_path.exists():
        return []
    
    latest = {}  # {sensor_id: reading}
    
    with open(csv_path, mode='r') as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            latest[row['sensor_id']] = SensorReading(
                timestamp=row['timestamp'],
                sensor_id=row['sensor_id'],
                sensor_type=row['sensor_type'],
                value=float(row['value']),
                unit=row['unit']
            )
    
    return list(latest.values())

# ============================================================================
# ROTAS - CONTROLE DOS SENSORES
# ============================================================================

@app.post("/sensors/{sensor_id}/pause", response_model=SensorStatus, tags=["Control"])
async def pause_sensor(sensor_id: str):
    """Pausa a coleta de dados de um sensor."""
    success = manager.pause_sensor(sensor_id)
    
    if not success:
        raise HTTPException(status_code=404, detail=f"Sensor {sensor_id} não encontrado")
    
    return SensorStatus(
        sensor_id=sensor_id,
        action="pause",
        success=True,
        message=f"Sensor {sensor_id} pausado com sucesso"
    )

@app.post("/sensors/{sensor_id}/resume", response_model=SensorStatus, tags=["Control"])
async def resume_sensor(sensor_id: str):
    """Resume a coleta de dados de um sensor."""
    success = manager.resume_sensor(sensor_id)
    
    if not success:
        raise HTTPException(status_code=404, detail=f"Sensor {sensor_id} não encontrado")
    
    return SensorStatus(
        sensor_id=sensor_id,
        action="resume",
        success=True,
        message=f"Sensor {sensor_id} resumido com sucesso"
    )

@app.post("/sensors/{sensor_id}/stop", response_model=SensorStatus, tags=["Control"])
async def stop_sensor(sensor_id: str):
    """Para permanentemente um sensor."""
    success = manager.stop_sensor(sensor_id)
    
    if not success:
        raise HTTPException(status_code=404, detail=f"Sensor {sensor_id} não encontrado")
    
    return SensorStatus(
        sensor_id=sensor_id,
        action="stop",
        success=True,
        message=f"Sensor {sensor_id} parado permanentemente"
    )

# ============================================================================
# INFORMAÇÕES ADICIONAIS
# ============================================================================

@app.get("/stats/summary", tags=["Stats"])
async def get_summary():
    """Retorna resumo geral do sistema."""
    csv_path = Path("data/raw_data.csv")
    
    if not csv_path.exists():
        return {
            "total_readings": 0,
            "active_sensors": manager.get_active_count(),
            "sensors": []
        }
    
    # Contar leituras por sensor
    sensor_counts = {}
    
    with open(csv_path, mode='r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            sensor_id = row['sensor_id']
            sensor_counts[sensor_id] = sensor_counts.get(sensor_id, 0) + 1
    
    return {
        "total_readings": sum(sensor_counts.values()),
        "active_sensors": manager.get_active_count(),
        "sensors": [
            {"sensor_id": sid, "reading_count": count}
            for sid, count in sensor_counts.items()
        ]
    }

# ============================================================================
# ROTAS - ETL
# ============================================================================

@app.post("/etl/run", tags=["ETL"])
async def run_etl_pipeline():
    """
    Executa pipeline ETL manualmente.
    
    Processo:
    1. Extrai dados de raw_data.csv
    2. Limpa e transforma dados
    3. Salva em processed.csv
    """
    success = etl.run()
    
    if success:
        stats = etl.get_processing_stats()
        return {
            "status": "success",
            "message": "Pipeline ETL executado com sucesso",
            "stats": stats
        }
    else:
        raise HTTPException(
            status_code=500, 
            detail="Erro ao executar pipeline ETL"
        )

@app.get("/etl/stats", tags=["ETL"])
async def get_etl_stats():
    """Retorna estatísticas sobre dados processados."""
    return etl.get_processing_stats()

# ============================================================================
# ROTAS - ANALYTICS
# ============================================================================

@app.get("/analytics/{sensor_id}/statistics", tags=["Analytics"])
async def get_sensor_statistics(sensor_id: str):
    """
    Retorna estatísticas descritivas de um sensor.
    
    Inclui: média, mediana, desvio padrão, min, max, quartis, etc.
    """
    stats = analytics.calculate_statistics(sensor_id)
    
    if "error" in stats:
        raise HTTPException(status_code=404, detail=stats["error"])
    
    return stats

@app.get("/analytics/{sensor_id}/outliers", tags=["Analytics"])
async def get_outliers(
    sensor_id: str,
    method: str = Query("iqr", regex="^(iqr|zscore)$", description="Método de detecção: iqr ou zscore")
):
    """
    Detecta outliers nos dados de um sensor.
    
    Métodos disponíveis:
    - **iqr**: Interquartile Range (padrão)
    - **zscore**: Z-score (3 desvios padrão)
    """
    outliers = analytics.detect_outliers(sensor_id, method=method)
    
    if "error" in outliers:
        raise HTTPException(status_code=404, detail=outliers["error"])
    
    return outliers

@app.get("/analytics/{sensor_id}/trend", tags=["Analytics"])
async def get_trend(
    sensor_id: str,
    window: int = Query(10, ge=2, le=100, description="Tamanho da janela para média móvel")
):
    """
    Retorna análise de tendência usando média móvel.
    
    - **window**: Tamanho da janela (padrão: 10 leituras)
    """
    trend = analytics.get_trend(sensor_id, window=window)
    
    if "error" in trend:
        raise HTTPException(status_code=404, detail=trend["error"])
    
    return trend