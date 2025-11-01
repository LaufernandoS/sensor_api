# sensors/simulator.py
import random
import time
import csv
from threading import Thread
from datetime import datetime

class Sensor(Thread):
    def __init__(self, sensor_id, sensor_type):
        super().__init__(daemon=True)
        self.sensor_id = sensor_id
        self.sensor_type = sensor_type
        self.running = True
    
    def run(self):
        while self.running:
            value = self._generate_value()
            self._save_reading(value)
            time.sleep(2)  # Lê a cada 2s
    
    def _generate_value(self):
        # Implementar ranges por tipo
        pass
    
    def _save_reading(self, value):
        # Append em raw_data.csv
        pass

if __name__ == "__main__":
    # Testar com 2 sensores
    pass