# app.py
from fastapi import FastAPI
import pandas as pd

app = FastAPI(title="Sensor Monitoring API")

@app.get("/sensors")
def get_all_sensors():
    df = pd.read_csv("data/raw_data.csv")
    latest = df.groupby('sensor_id').last().reset_index()
    return latest.to_dict(orient="records")