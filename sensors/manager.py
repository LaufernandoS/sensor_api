# sensors/manager.py
from sensors.simulator import Sensor

class SensorManager:
    def __init__(self):
        self.sensors = {}
    
    def add_sensor(self, sensor):
        self.sensors[sensor.sensor_id] = sensor
        sensor.start()
    
    def get_sensor(self, sensor_id):
        return self.sensors.get(sensor_id)
    
    def shutdown_all(self):
        Sensor.stop_all()
