import pandas as pd
from pathlib import Path
from datetime import datetime
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ETLPipeline:
    """Pipeline ETL para processamento de dados de sensores."""
    
    def __init__(self, raw_path: str = "data/raw_data.csv", 
                 processed_path: str = "data/processed.csv"):
        self.raw_path = Path(raw_path)
        self.processed_path = Path(processed_path)
        
        # Criar diretório se não existir
        self.processed_path.parent.mkdir(exist_ok=True)
    
    def extract(self) -> pd.DataFrame:
        """
        EXTRACT: Lê dados brutos do CSV.
        
        Returns:
            DataFrame com dados brutos ou vazio se arquivo não existir
        """
        if not self.raw_path.exists():
            logger.warning(f"Arquivo {self.raw_path} não encontrado")
            return pd.DataFrame()
        
        try:
            df = pd.read_csv(self.raw_path)
            logger.info(f"✓ Extraídas {len(df)} linhas de {self.raw_path}")
            return df
        except Exception as e:
            logger.error(f"Erro ao extrair dados: {e}")
            return pd.DataFrame()
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        TRANSFORM: Limpa e transforma os dados.
        
        Transformações aplicadas:
        1. Remove duplicatas exatas
        2. Remove linhas com valores nulos críticos
        3. Converte timestamp para datetime
        4. Valida ranges de valores por tipo de sensor
        5. Ordena por timestamp
        6. Adiciona coluna de data/hora separadas
        
        Args:
            df: DataFrame bruto
            
        Returns:
            DataFrame limpo e transformado
        """
        if df.empty:
            logger.warning("DataFrame vazio, pulando transformação")
            return df
        
        original_count = len(df)
        
        # 1. Remover duplicatas exatas
        df = df.drop_duplicates()
        logger.info(f"  - Removidas {original_count - len(df)} duplicatas")
        
        # 2. Remover linhas com valores nulos críticos
        df = df.dropna(subset=['timestamp', 'sensor_id', 'value'])
        logger.info(f"  - Removidas {original_count - len(df)} linhas com nulos")
        
        # 3. Converter timestamp para datetime
        try:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
        except Exception as e:
            logger.error(f"Erro ao converter timestamp: {e}")
            return pd.DataFrame()
        
        # 4. Validar ranges por tipo de sensor
        df = self._validate_sensor_ranges(df)
        
        # 5. Ordenar por timestamp
        df = df.sort_values('timestamp').reset_index(drop=True)
        
        # 6. Adicionar colunas derivadas
        df['date'] = df['timestamp'].dt.date
        df['hour'] = df['timestamp'].dt.hour
        df['minute'] = df['timestamp'].dt.minute
        
        logger.info(f"✓ Transformação concluída: {len(df)} linhas válidas")
        return df
    
    def _validate_sensor_ranges(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Valida e remove outliers absurdos baseado em ranges físicos.
        
        Ranges considerados:
        - temperature: -50°C a 100°C (sensores comuns)
        - humidity: 0% a 100%
        - noise: 0dB a 140dB (limiar de dor)
        """
        initial_count = len(df)
        
        # Definir ranges válidos
        ranges = {
            'temperature': (-50, 100),
            'humidity': (0, 100),
            'noise': (0, 140)
        }
        
        # Aplicar validação
        for sensor_type, (min_val, max_val) in ranges.items():
            mask = (df['sensor_type'] == sensor_type) & \
                   ((df['value'] < min_val) | (df['value'] > max_val))
            
            invalid_count = mask.sum()
            if invalid_count > 0:
                logger.warning(f"  - Removidos {invalid_count} valores inválidos de {sensor_type}")
            
            df = df[~mask]
        
        removed = initial_count - len(df)
        if removed > 0:
            logger.info(f"  - Total de {removed} valores fora de range removidos")
        
        return df
    
    def load(self, df: pd.DataFrame) -> bool:
        """
        LOAD: Salva dados processados no CSV.
        
        Args:
            df: DataFrame processado
            
        Returns:
            True se salvo com sucesso, False caso contrário
        """
        if df.empty:
            logger.warning("DataFrame vazio, nada para salvar")
            return False
        
        try:
            df.to_csv(self.processed_path, index=False)
            logger.info(f"✓ Salvos {len(df)} registros em {self.processed_path}")
            return True
        except Exception as e:
            logger.error(f"Erro ao salvar dados processados: {e}")
            return False
    
    def run(self) -> bool:
        """
        Executa pipeline ETL completo.
        
        Returns:
            True se pipeline executou com sucesso
        """
        logger.info(f"\n{'='*60}")
        logger.info(f"Iniciando pipeline ETL - {datetime.now()}")
        logger.info(f"{'='*60}\n")
        
        # Extract
        logger.info("📥 EXTRACT: Extraindo dados brutos...")
        df = self.extract()
        
        if df.empty:
            logger.warning("Nenhum dado para processar")
            return False
        
        # Transform
        logger.info(f"\n🔄 TRANSFORM: Transformando dados...")
        df_clean = self.transform(df)
        
        if df_clean.empty:
            logger.warning("Nenhum dado válido após transformação")
            return False
        
        # Load
        logger.info(f"\n💾 LOAD: Salvando dados processados...")
        success = self.load(df_clean)
        
        if success:
            logger.info(f"\n{'='*60}")
            logger.info(f"✓ Pipeline ETL concluído com sucesso!")
            logger.info(f"  - Dados originais: {len(df)}")
            logger.info(f"  - Dados processados: {len(df_clean)}")
            logger.info(f"  - Taxa de retenção: {len(df_clean)/len(df)*100:.1f}%")
            logger.info(f"{'='*60}\n")
        
        return success
    
    def get_processing_stats(self) -> dict:
        """Retorna estatísticas sobre os dados processados."""
        if not self.processed_path.exists():
            return {
                "status": "no_data",
                "message": "Dados processados não encontrados"
            }
        
        try:
            df = pd.read_csv(self.processed_path)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            return {
                "status": "success",
                "total_records": len(df),
                "sensors": df['sensor_id'].nunique(),
                "sensor_types": df['sensor_type'].unique().tolist(),
                "date_range": {
                    "start": df['timestamp'].min().isoformat(),
                    "end": df['timestamp'].max().isoformat()
                },
                "records_per_sensor": df.groupby('sensor_id').size().to_dict()
            }
        except Exception as e:
            return {
                "status": "error",
                "message": str(e)
            }


# Função helper para executar ETL standalone
def run_etl():
    """Executa pipeline ETL uma vez."""
    pipeline = ETLPipeline()
    pipeline.run()


if __name__ == "__main__":
    # Teste standalone
    run_etl()