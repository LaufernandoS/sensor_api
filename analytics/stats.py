import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)


class SensorAnalytics:
    """Análise estatística de dados de sensores."""
    
    def __init__(self, data_path: str = "data/processed.csv"):
        self.data_path = Path(data_path)
    
    def _load_data(self, sensor_id: Optional[str] = None) -> pd.DataFrame:
        """Carrega dados processados, opcionalmente filtrando por sensor."""
        if not self.data_path.exists():
            logger.warning(f"Arquivo {self.data_path} não encontrado")
            return pd.DataFrame()
        
        df = pd.read_csv(self.data_path)
        
        if sensor_id:
            df = df[df['sensor_id'] == sensor_id]
        
        return df
    
    def calculate_statistics(self, sensor_id: str) -> Dict:
        """
        Calcula estatísticas descritivas para um sensor.
        
        Métricas calculadas:
        - Média (mean)
        - Mediana (median)
        - Desvio padrão (std)
        - Mínimo e máximo
        - Quartis (Q1, Q3)
        - Coeficiente de variação
        
        Args:
            sensor_id: ID do sensor
            
        Returns:
            Dicionário com estatísticas
        """
        df = self._load_data(sensor_id)
        
        if df.empty:
            return {
                "error": f"Nenhum dado encontrado para sensor {sensor_id}"
            }
        
        values = df['value']
        
        stats = {
            "sensor_id": sensor_id,
            "sensor_type": df['sensor_type'].iloc[0],
            "unit": df['unit'].iloc[0],
            "count": int(len(values)),
            "mean": float(values.mean()),
            "median": float(values.median()),
            "std": float(values.std()),
            "min": float(values.min()),
            "max": float(values.max()),
            "q1": float(values.quantile(0.25)),
            "q3": float(values.quantile(0.75)),
            "range": float(values.max() - values.min()),
            "coefficient_of_variation": float((values.std() / values.mean()) * 100) if values.mean() != 0 else 0
        }
        
        return stats
    
    def detect_outliers(self, sensor_id: str, method: str = "iqr") -> Dict:
        """
        Detecta outliers usando método IQR ou Z-score.
        
        Métodos:
        - IQR (Interquartile Range): valores fora de [Q1 - 1.5*IQR, Q3 + 1.5*IQR]
        - Z-score: valores com |z| > 3 (3 desvios padrão)
        
        Args:
            sensor_id: ID do sensor
            method: "iqr" ou "zscore"
            
        Returns:
            Dicionário com outliers detectados
        """
        df = self._load_data(sensor_id)
        
        if df.empty:
            return {
                "error": f"Nenhum dado encontrado para sensor {sensor_id}"
            }
        
        values = df['value']
        
        if method == "iqr":
            outliers_mask = self._detect_outliers_iqr(values)
            method_name = "IQR"
        elif method == "zscore":
            outliers_mask = self._detect_outliers_zscore(values)
            method_name = "Z-score"
        else:
            return {"error": f"Método '{method}' inválido. Use 'iqr' ou 'zscore'"}
        
        outliers = df[outliers_mask]
        
        return {
            "sensor_id": sensor_id,
            "method": method_name,
            "total_readings": int(len(df)),
            "outliers_count": int(len(outliers)),
            "outliers_percentage": float((len(outliers) / len(df)) * 100),
            "outliers": [
                {
                    "timestamp": row['timestamp'],
                    "value": float(row['value']),
                    "unit": row['unit']
                }
                for _, row in outliers.iterrows()
            ][:50]  # Limitar a 50 outliers para não sobrecarregar resposta
        }
    
    def _detect_outliers_iqr(self, values: pd.Series) -> pd.Series:
        """Detecta outliers usando método IQR."""
        Q1 = values.quantile(0.25)
        Q3 = values.quantile(0.75)
        IQR = Q3 - Q1
        
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        
        return (values < lower_bound) | (values > upper_bound)
    
    def _detect_outliers_zscore(self, values: pd.Series) -> pd.Series:
        """Detecta outliers usando Z-score (>3 desvios padrão)."""
        z_scores = np.abs((values - values.mean()) / values.std())
        return z_scores > 3
    
    def get_trend(self, sensor_id: str, window: int = 10) -> Dict:
        """
        Calcula tendência dos dados usando média móvel.
        
        Args:
            sensor_id: ID do sensor
            window: Tamanho da janela para média móvel
            
        Returns:
            Dicionário com informações de tendência
        """
        df = self._load_data(sensor_id)
        
        if df.empty:
            return {
                "error": f"Nenhum dado encontrado para sensor {sensor_id}"
            }
        
        df = df.sort_values('timestamp')
        df['moving_avg'] = df['value'].rolling(window=window, min_periods=1).mean()
        
        # Calcular tendência (diferença entre últimas e primeiras médias)
        first_avg = df['moving_avg'].iloc[:window].mean()
        last_avg = df['moving_avg'].iloc[-window:].mean()
        trend_diff = last_avg - first_avg
        
        if abs(trend_diff) < 0.01:
            trend = "stable"
        elif trend_diff > 0:
            trend = "increasing"
        else:
            trend = "decreasing"
        
        return {
            "sensor_id": sensor_id,
            "window_size": window,
            "trend": trend,
            "trend_value": float(trend_diff),
            "first_period_avg": float(first_avg),
            "last_period_avg": float(last_avg),
            "data_points": [
                {
                    "timestamp": row['timestamp'],
                    "value": float(row['value']),
                    "moving_avg": float(row['moving_avg'])
                }
                for _, row in df.tail(50).iterrows()  # Últimas 50 leituras
            ]
        }
    
    def compare_sensors(self, sensor_type: Optional[str] = None) -> Dict:
        """
        Compara estatísticas entre sensores do mesmo tipo.
        
        Args:
            sensor_type: Filtrar por tipo (opcional)
            
        Returns:
            Dicionário com comparação entre sensores
        """
        df = self._load_data()
        
        if df.empty:
            return {"error": "Nenhum dado encontrado"}
        
        if sensor_type:
            df = df[df['sensor_type'] == sensor_type]
        
        if df.empty:
            return {"error": f"Nenhum dado encontrado para tipo {sensor_type}"}
        
        # Agrupar por sensor
        comparison = []
        
        for sensor_id in df['sensor_id'].unique():
            sensor_data = df[df['sensor_id'] == sensor_id]
            values = sensor_data['value']
            
            comparison.append({
                "sensor_id": sensor_id,
                "sensor_type": sensor_data['sensor_type'].iloc[0],
                "count": int(len(values)),
                "mean": float(values.mean()),
                "std": float(values.std()),
                "min": float(values.min()),
                "max": float(values.max())
            })
        
        return {
            "sensor_type": sensor_type or "all",
            "sensors_count": len(comparison),
            "comparison": sorted(comparison, key=lambda x: x['mean'])
        }
    
    def get_time_series(self, sensor_id: str, 
                       aggregation: str = "1H") -> Dict:
        """
        Retorna série temporal agregada.
        
        Args:
            sensor_id: ID do sensor
            aggregation: Frequência de agregação (ex: "1H", "5T", "1D")
                        T = minutos, H = horas, D = dias
            
        Returns:
            Série temporal agregada
        """
        df = self._load_data(sensor_id)
        
        if df.empty:
            return {
                "error": f"Nenhum dado encontrado para sensor {sensor_id}"
            }
        
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.set_index('timestamp')
        
        # Agregar por período
        aggregated = df['value'].resample(aggregation).agg(['mean', 'min', 'max', 'count'])
        aggregated = aggregated.dropna()
        
        return {
            "sensor_id": sensor_id,
            "aggregation": aggregation,
            "data_points": [
                {
                    "timestamp": idx.isoformat(),
                    "mean": float(row['mean']),
                    "min": float(row['min']),
                    "max": float(row['max']),
                    "count": int(row['count'])
                }
                for idx, row in aggregated.iterrows()
            ]
        }


# Função helper para testar analytics
def test_analytics():
    """Testa módulo de analytics."""
    analytics = SensorAnalytics()
    
    # Testar estatísticas
    print("\n=== Estatísticas TEMP-001 ===")
    stats = analytics.calculate_statistics("TEMP-001")
    print(stats)
    
    # Testar detecção de outliers
    print("\n=== Outliers (IQR) ===")
    outliers = analytics.detect_outliers("TEMP-001", method="iqr")
    print(f"Encontrados {outliers.get('outliers_count', 0)} outliers")
    
    # Testar tendência
    print("\n=== Tendência ===")
    trend = analytics.get_trend("TEMP-001")
    print(f"Tendência: {trend.get('trend', 'N/A')}")


if __name__ == "__main__":
    test_analytics()