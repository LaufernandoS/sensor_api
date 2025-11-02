# 🌡️ Sensor Monitoring and Analytics API

Sistema de monitoramento IoT com simulação de sensores, pipeline ETL e análise estatística em tempo real.

## 🎯 Objetivo
Projeto desenvolvido para preparação técnica focada em:
- Programação concorrente (threads Python)
- APIs RESTful com FastAPI
- Pipeline ETL com pandas
- Análise de séries temporais

## 🏗️ Arquitetura
```
Sensores (threads) → CSV bruto → ETL → CSV processado
                         ↓
                    FastAPI (consultas + analytics)
```

## 📊 Sensores Simulados
- **Temperatura**: Distribuição normal (22°C ± 3°C)
- **Umidade**: Distribuição beta (60-80% zona típica)
- **Ruído**: Log-normal (ruído urbano, picos raros)

## 🚀 Quick Start

### Testar simulador standalone:
```bash
python -m sensors.simulator
```

Gera dados em `data/raw_data.csv` por 30 segundos com controles de pausa/stop.

### Iniciar API:
```bash
uvicorn app:app --reload
```

Acesse: http://localhost:8000/docs

## 📂 Estrutura do Projeto
```
sensor_api/
├── sensors/
│   └── simulator.py    # ✓ Simulação multi-threaded
├── data/
│   ├── raw_data.csv    # Dados brutos (gerado)
│   └── processed.csv   # Dados limpos (ETL)
├── etl/                # 🚧 Em desenvolvimento
├── analytics/          # 🚧 Em desenvolvimento
└── app.py              # 🚧 Em desenvolvimento
```

## 🧪 Tecnologias
- Python 3.10+
- FastAPI
- Pandas
- Threading (stdlib)

---

**Status**: 🟢 Fase 1/4 - Simulação implementada