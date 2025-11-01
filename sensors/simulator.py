import random
import time
import csv
from threading import Thread, Event, Lock
from datetime import datetime
from pathlib import Path

class Sensor(Thread):
    """
    Simula um sensor IoT gerando dados realistas em thread.
    
    Tipos suportados:
    - temperature: Distribuição normal (~22°C, desvio 3°C)
    - humidity: Beta distribution (60-80% típico)
    - noise: Log-normal (dB, mais valores baixos, alguns picos)
    """
    
    # Controle global de threads ativas
    active_sensors = []
    sensors_lock = Lock()
    
    # # Event é como uma porta:
    # event = Event()     # Porta FECHADA 🔴
    # event.set()         # Porta ABRE 🟢
    # event.clear()       # Porta FECHA 🔴
    # event.wait()        # Espera porta abrir (bloqueia se fechada)
    # event.is_set()      # Verifica: porta está aberta?
    
    def __init__(self, sensor_id: str, sensor_type: str, interval: float = 2.0):
        super().__init__(daemon=True)
        self.sensor_id = sensor_id
        self.sensor_type = sensor_type
        self.interval = interval
        self._stop_event = Event()
        self._pause_event = Event()
        self._pause_event.set()  # Não pausado inicialmente
        
        # Registrar sensor ativo
        with Sensor.sensors_lock:
            Sensor.active_sensors.append(self)
    
    def run(self):
        """Loop principal da thread - gera e salva leituras."""
        print(f"[{self.sensor_id}] Sensor {self.sensor_type} iniciado")
        
        while not self._stop_event.is_set():
            self._pause_event.wait()  # Bloqueia se pausado
            
            value = self._generate_value()
            self._save_reading(value)
            
            time.sleep(self.interval)
        
        print(f"[{self.sensor_id}] Sensor finalizado")
        self._unregister()
    
    def _generate_value(self) -> float:
        """Gera valor baseado em distribuições realistas."""
        if self.sensor_type == "temperature":
            # Normal: média 22°C, desvio padrão 3°C
            # Simula ambiente interno com variação natural
            return round(random.gauss(22, 3), 2)
        
        elif self.sensor_type == "humidity":
            # Beta distribution transformada para 40-90%
            # Concentra valores entre 60-80% (confortável)
            beta_value = random.betavariate(5, 3)  # Pico em ~62%
            return round(40 + beta_value * 50, 2)
        
        elif self.sensor_type == "noise":
            # Log-normal: maioria baixo (~40dB), poucos picos (>80dB)
            # Simula ruído urbano/industrial
            log_value = random.lognormvariate(3.7, 0.4)
            return round(min(log_value, 120), 2)  # Cap em 120dB
        
        else:
            raise ValueError(f"Tipo de sensor desconhecido: {self.sensor_type}")
    
    def _save_reading(self, value: float):
        """Salva leitura no CSV (modo append)."""
        data_dir = Path("data")
        data_dir.mkdir(exist_ok=True)
        
        csv_path = data_dir / "raw_data.csv"
        
        # Criar header se arquivo não existe
        file_exists = csv_path.exists()
        
        with open(csv_path, mode='a', newline='') as f:
            writer = csv.writer(f)
            
            if not file_exists:
                writer.writerow(['timestamp', 'sensor_id', 'sensor_type', 'value', 'unit'])
            
            writer.writerow([
                datetime.now().isoformat(),
                self.sensor_id,
                self.sensor_type,
                value,
                self._get_unit()
            ])
    
    def _get_unit(self) -> str:
        """Retorna unidade de medida do sensor."""
        units = {
            'temperature': '°C',
            'humidity': '%',
            'noise': 'dB'
        }
        return units.get(self.sensor_type, 'unknown')
    
    def pause(self):
        """Pausa a coleta de dados (thread continua ativa)."""
        self._pause_event.clear()
        print(f"[{self.sensor_id}] Pausado")
    
    def resume(self):
        """Resume a coleta de dados."""
        self._pause_event.set()
        print(f"[{self.sensor_id}] Resumido")
    
    def stop(self):
        """Para a thread permanentemente."""
        self._stop_event.set()
        self._pause_event.set()  # Desbloquear se estiver pausado
    
    def _unregister(self):
        """Remove sensor da lista de ativos."""
        with Sensor.sensors_lock:
            if self in Sensor.active_sensors:
                Sensor.active_sensors.remove(self)
    
    @classmethod
    def stop_all(cls):
        """Para todos os sensores ativos."""
        with cls.sensors_lock:
            for sensor in cls.active_sensors[:]:  # Cópia da lista
                sensor.stop()
    
    @classmethod
    def get_active_count(cls) -> int:
        """Retorna número de sensores ativos."""
        with cls.sensors_lock:
            return len(cls.active_sensors)


def main():
    """Teste standalone: simula 2 sensores por 30 segundos."""
    print("=== Iniciando simulação de sensores ===\n")
    
    # Criar sensores
    sensor1 = Sensor("TEMP-001", "temperature", interval=2.0)
    sensor2 = Sensor("HUM-001", "humidity", interval=2.5)
    sensor3 = Sensor("NOISE-001", "noise", interval=3.0)
    
    # Iniciar threads
    sensor1.start()
    sensor2.start()
    sensor3.start()
    
    try:
        # Simular 10 segundos
        time.sleep(10)
        
        # Pausar sensor de temperatura
        print("\n--- Pausando sensor de temperatura ---")
        sensor1.pause()
        time.sleep(5)
        
        # Resumir
        print("\n--- Resumindo sensor de temperatura ---")
        sensor1.resume()
        time.sleep(5)
        
        # Parar um sensor
        print("\n--- Parando sensor de ruído ---")
        sensor3.stop()
        time.sleep(5)
        
    except KeyboardInterrupt:
        print("\n\n[CTRL+C] Encerrando simulação...")
    
    finally:
        # Parar todos os sensores
        print(f"\nParando {Sensor.get_active_count()} sensores ativos...")
        Sensor.stop_all()
        
        # Aguardar threads finalizarem
        time.sleep(1)
        print("\n✓ Simulação finalizada")
        print(f"✓ Dados salvos em: data/raw_data.csv")


if __name__ == "__main__":
    main()