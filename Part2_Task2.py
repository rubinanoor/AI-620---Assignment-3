from typing import Optional
import os     
import joblib
from contextlib import asynccontextmanager
from fastapi             import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware  # allows Streamlit to call us
from pydantic            import BaseModel, Field, field_validator
import numpy as np
import pandas as pd

MODEL_PATH = "/Users/BABARHUSSAIN/Desktop/Study Material/MS AI/Data Engineering/Assignment 3/pakwheels_svm_model.pkl"
model_artefact = None  

# lifespan handler
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Handles startup and shutdown logic in one place."""
    global model_artefact
    
    
    print("Checking for model file...")
    if not os.path.exists(MODEL_PATH):
        print(f"ERROR: Model file not found at {MODEL_PATH}")
    else:
        try:
            model_artefact = joblib.load(MODEL_PATH)
            print(f"Successfully loaded model from {MODEL_PATH}")
            print(f"Features expected: {model_artefact.get('features', 'Unknown')}")
        except Exception as e:
            print(f"Failed to load model: {e}")

    yield  #  running 
    
    # shutdown
    print("Shutting down API: Cleaning up resources...")

# initialize FastAPI with the lifespan
app = FastAPI(
    title       = "PakWheels Price Category API",
    description = "Predicts whether a used car is HIGH or LOW price using a trained SVM.",
    version     = "1.0.0",
    lifespan    = lifespan
)

# 4. Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  
    allow_headers=["*"],
)

class CarFeatures(BaseModel):

    year: int = Field(
        ...,
        ge=1980, le=2024,
        description="Year of manufacture (1980–2024)",
        examples=[2018], # V2 uses 'examples' list
    )
    engine: float = Field(
        ...,
        ge=600, le=8000,
        description="Engine capacity in cc (600–8000)",
        examples=[1300],
    )
    mileage: int = Field(
        ...,
        ge=0, le=500_000,
        description="Odometer reading in km (0–500,000)",
        examples=[45000],
    )
    transmission: str = Field(
        ...,
        description="Transmission type: 'Manual' or 'Automatic'",
        examples=["Manual"],
    )
    fuel: str = Field(
        ...,
        description="Fuel type: 'Petrol', 'Diesel', 'Hybrid', or 'CNG'",
        examples=["Petrol"],
    )
    body: Optional[str] = Field(
        default="Sedan",
        description="Body type (optional)",
        examples=["Sedan"],
    )
    city: Optional[str] = Field(
        default="Karachi",
        description="City where the car is listed (optional)",
        examples=["Karachi"],
    )

    # Modern V2 Validator
    @field_validator("transmission")
    @classmethod 
    def normalise_transmission(cls, v: str) -> str:
        v = v.strip().title()
        if v not in ("Manual", "Automatic"):
            raise ValueError("transmission must be 'Manual' or 'Automatic'")
        return v

    @field_validator("fuel")
    @classmethod
    def normalise_fuel(cls, v: str) -> str:
        v = v.strip().title()
       
        if v == "Cng": v = "CNG" 
        if v not in ("Petrol", "Diesel", "Hybrid", "CNG", "Electric"):
            raise ValueError("fuel must be one of: Petrol, Diesel, Hybrid, CNG, Electric")
        return v


class PredictionResponse(BaseModel):
    price_category  : str   = Field(..., description="'High Price' or 'Low Price'")
    probability_high: float = Field(..., description="Confidence score for High Price class")
    probability_low : float = Field(..., description="Confidence score for Low Price class")
    median_price_pkr: float = Field(..., description="Threshold price (PKR) used for categorisation")
    input_received  : dict  = Field(..., description="Echo of the normalised input features")

    # 4. HELPER — Feature Engineering & Encoding

def prepare_features(car: CarFeatures) -> pd.DataFrame:
  
    artefact      = model_artefact
    label_encoders = artefact["label_encoders"]
    features       = artefact["features"]
    current_year   = artefact["current_year"]

    car_age          = current_year - car.year
    mileage_per_year = car.mileage / (car_age + 1)   # +1 avoids div-by-zero

   
    raw = {
        "car_age"         : car_age,
        "engine"          : car.engine,
        "mileage"         : car.mileage,
        "mileage_per_year": mileage_per_year,
        "transmission"    : car.transmission,
        "fuel"            : car.fuel,
        "body"            : car.body or "Sedan",
        "city"            : car.city or "Karachi",
    }

    df_row = pd.DataFrame([raw])

    for col in ["transmission", "fuel", "body", "city"]:
        le = label_encoders.get(col)
        if le is not None:
            val = df_row[col].iloc[0]
            if val in le.classes_:
                df_row[col] = le.transform([val])
            else:
                df_row[col] = 0

    return df_row[features]

#endpoints

@app.get("/", tags=["Root"])
def root():
    return {"message": "PakWheels Price Category API is running. Visit /docs for usage."}


@app.get("/health", tags=["Health"])
def health_check():
    model_loaded = model_artefact is not None
    return {
        "status"      : "healthy" if model_loaded else "unhealthy",
        "model_loaded": model_loaded,
    }


@app.post("/predict", response_model=PredictionResponse, tags=["Prediction"])
def predict(car: CarFeatures):
   
    if model_artefact is None:
        raise HTTPException(status_code=503, detail="Model not loaded yet. Retry in a moment.")

    try:
        X = prepare_features(car)

        pipeline      = model_artefact["pipeline"]
        pred_class    = pipeline.predict(X)[0]         # 0=Low, 1=High
        pred_proba    = pipeline.predict_proba(X)[0]   # [P(Low), P(High)]
        median_price  = model_artefact["median_price"]

        label = "High Price" if pred_class == 1 else "Low Price"

        return PredictionResponse(
            price_category   = label,
            probability_high = round(float(pred_proba[1]), 4),
            probability_low  = round(float(pred_proba[0]), 4),
            median_price_pkr = float(median_price),
            input_received   = car.dict(),
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Prediction error: {str(e)}")


@app.get("/model-info", tags=["Metadata"])
def model_info():
    if model_artefact is None:
        raise HTTPException(status_code=503, detail="Model not loaded.")
    return {
        "features"        : model_artefact["features"],
        "median_price_pkr": model_artefact["median_price"],
        "current_year"    : model_artefact["current_year"],
    }
if __name__ == "__main__":
    import uvicorn
    # In a local .py file, this is all you need:
    uvicorn.run(app, host="127.0.0.1", port=8000)