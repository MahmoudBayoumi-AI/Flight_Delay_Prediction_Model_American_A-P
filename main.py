from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import joblib
import numpy as np

app = FastAPI()

model      = joblib.load("lastmodel/RF_InFlight.joblib")
le_airports = joblib.load("lastmodel/le_airports.joblib")

class FlightInput(BaseModel):
    Year: int
    Quarter: int
    Month: int
    DayofMonth: int
    Origin: str
    Dest: str
    DepTime: float
    DepDelayMinutes: float
    DepDel15: int
    CRSDepTime: float
    tempF: float
    WindChillF: float
    humidity: float
    windspeedKmph: float
    WindGustKmph: float
    winddirDegree: float
    weatherCode: float
    visibility: float
    pressure: float
    cloudcover: float
    DewPointF: float
    time: int

@app.post("/predict")
def predict(data: FlightInput):
    try:
        origin_encoded = int(le_airports.transform([data.Origin])[0])
    except ValueError:
        raise HTTPException(
            status_code=422,
            detail=f"Origin '{data.Origin}' غير موجود. المطارات المتاحة: {le_airports.classes_.tolist()}"
        )

    try:
        dest_encoded = int(le_airports.transform([data.Dest])[0])
    except ValueError:
        raise HTTPException(
            status_code=422,
            detail=f"Dest '{data.Dest}' غير موجود. المطارات المتاحة: {le_airports.classes_.tolist()}"
        )

    features = np.array([[
        data.Year, data.Quarter, data.Month, data.DayofMonth,
        origin_encoded, dest_encoded,
        data.DepTime, data.DepDelayMinutes, data.DepDel15,
        data.CRSDepTime, data.tempF, data.WindChillF,
        data.humidity, data.windspeedKmph, data.WindGustKmph,
        data.winddirDegree, data.weatherCode, data.visibility,
        data.pressure, data.cloudcover, data.DewPointF, data.time
    ]])

    prediction = model.predict(features)[0]

    return {
        "predicted_delay_minutes": round(float(prediction), 2)
    }

@app.get("/airports")
def get_airports():
    airports = le_airports.classes_.tolist()
    return {"airports": airports}

@app.get("/health")
def health():
    return {"status": "ok"}