from pydantic import BaseModel, Field
from typing import Literal

class SensorReading(BaseModel):
    """Representa uma leitura individual de sensor."""
    timestamp: str = Field(..., description="ISO 8601 timestamp")
    sensor_id: str = Field(..., description="Identificador único do sensor")
    sensor_type: Literal["temperature", "humidity", "noise"] = Field(
        ..., 
        description="Tipo do sensor"
    )
    value: float = Field(..., description="Valor medido")
    unit: str = Field(..., description="Unidade de medida (°C, %, dB)")
    
    class Config:
        json_schema_extra = {
            "example": {
                "timestamp": "2025-11-02T14:30:00.123456",
                "sensor_id": "TEMP-001",
                "sensor_type": "temperature",
                "value": 23.5,
                "unit": "°C"
            }
        }


class SensorInfo(BaseModel):
    """Informações sobre um sensor registrado."""
    sensor_id: str = Field(..., description="Identificador único")
    sensor_type: Literal["temperature", "humidity", "noise"]
    status: Literal["running", "paused", "stopped"] = Field(
        ..., 
        description="Estado atual do sensor"
    )
    interval: float = Field(..., description="Intervalo entre leituras (segundos)")
    
    class Config:
        json_schema_extra = {
            "example": {
                "sensor_id": "TEMP-001",
                "sensor_type": "temperature",
                "status": "running",
                "interval": 2.0
            }
        }


class SensorStatus(BaseModel):
    """Resposta de operações de controle (pause/resume/stop)."""
    sensor_id: str
    action: Literal["pause", "resume", "stop"]
    success: bool
    message: str
    
    class Config:
        json_schema_extra = {
            "example": {
                "sensor_id": "TEMP-001",
                "action": "pause",
                "success": True,
                "message": "Sensor TEMP-001 pausado com sucesso"
            }
        }