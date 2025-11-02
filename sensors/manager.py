# sensors/manager.py
from sensors.simulator import Sensor

class SensorManager:
    def __init__(self):
        self.sensors = {}
    
    def add_sensor(self, sensor: Sensor):
        """Adiciona e inicia um sensor."""
        self.sensors[sensor.sensor_id] = sensor
        sensor.start()
        return sensor
    
    def get_sensor(self, sensor_id: str) -> Sensor | None:
        """Retorna sensor pelo ID."""
        return self.sensors.get(sensor_id)
    
    def list_sensors(self) -> list[Sensor]:
        """Lista todos os sensores."""
        return list(self.sensors.values())
    
    def pause_sensor(self, sensor_id: str) -> bool:
        """Pausa um sensor específico."""
        sensor = self.get_sensor(sensor_id)
        if sensor:
            sensor.pause()
            return True
        return False
    
    def resume_sensor(self, sensor_id: str) -> bool:
        """Resume um sensor específico."""
        sensor = self.get_sensor(sensor_id)
        if sensor:
            sensor.resume()
            return True
        return False
    
    def stop_sensor(self, sensor_id: str) -> bool:
        """Para um sensor específico."""
        sensor = self.get_sensor(sensor_id)
        if sensor:
            sensor.stop()
            del self.sensors[sensor_id]
            return True
        return False
    
    def shutdown_all(self):
        """Para todos os sensores."""
        Sensor.stop_all()
        self.sensors.clear()
    
    def get_active_count(self) -> int:
        """Retorna número de sensores ativos."""
        return len(self.sensors)
