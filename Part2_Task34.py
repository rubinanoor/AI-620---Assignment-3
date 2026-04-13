
import json
import streamlit as st    
import requests          

# page config
st.set_page_config(
    page_title = "PakWheels Price Predictor",
    page_icon  = "🚗",
    layout     = "centered",
)

BACKEND_URL = "http://localhost:8000"   

TRANSMISSIONS = ["Manual", "Automatic"]
FUELS         = ["Petrol", "Diesel", "Hybrid", "CNG"]
BODIES        = ["Sedan", "Hatchback", "SUV", "Crossover", "Compact SUV",
                 "Van", "MPV", "Pickup", "Coupe"]
CITIES        = ["Karachi", "Lahore", "Islamabad", "Rawalpindi", "Faisalabad",
                 "Multan", "Peshawar", "Quetta", "Sialkot", "Gujranwala",
                 "Other"]

def call_predict_api(payload: dict) -> dict:

    try:
        response = requests.post(
            url     = f"{BACKEND_URL}/predict",
            json    = payload,
            timeout = 10,  
        )
        response.raise_for_status()  
        return response.json()

    except requests.exceptions.ConnectionError:
        st.error(
            "Cannot connect to the backend server.\n\n"
            f"Make sure FastAPI is running at `{BACKEND_URL}`.\n"
            "Command: `uvicorn Part2_Task2_backend:app --port 8000 --reload`"
        )
        return None

    except requests.exceptions.HTTPError as e:
        st.error(f"API error: {e.response.status_code} — {e.response.text}")
        return None

    except Exception as e:
        st.error(f"Unexpected error: {str(e)}")
        return None


def check_backend_health() -> bool:
   
    try:
        r = requests.get(f"{BACKEND_URL}/health", timeout=3)
        return r.status_code == 200 and r.json().get("model_loaded", False)
    except Exception:
        return False


# app layout

# header
st.title("PakWheels Car Price Predictor")
st.markdown(
    "Enter the car's specifications below. The system will predict whether "
    "the car belongs to a **High Price** or **Low Price** category based on "
    "a trained SVM classifier."
)
st.divider()

# backend status checker 

with st.sidebar:
    st.header("⚙️ System Status")
    if check_backend_health():
        st.success("Backend: Connected")
    else:
        st.error("Backend: Not reachable")
        st.code("uvicorn Part2_Task2_backend:app --port 8000 --reload")
    
    st.divider()
    st.markdown("**Model Info**")
    st.markdown("- Algorithm: Support Vector Machine (RBF kernel)")
    st.markdown("- Target: High / Low price (median split ~2.7M PKR)")
    st.markdown("- Features: Year, Engine, Mileage, Transmission, Fuel, Body, City")

# input form
with st.form("prediction_form"):
    st.subheader("Car Specifications")

    col1, col2 = st.columns(2)

    with col1:
        year = st.number_input(
            label    = "Year of Manufacture",
            min_value= 1980,
            max_value= 2024,
            value    = 2018,
            step     = 1,
            help     = "Manufacturing year (1980–2024)",
        )

        engine = st.number_input(
            label    = "Engine Capacity (cc)",
            min_value= 600,
            max_value= 8000,
            value    = 1300,
            step     = 100,
            help     = "Engine displacement in cubic centimetres",
        )

        mileage = st.number_input(
            label    = "Mileage (km)",
            min_value= 0,
            max_value= 500_000,
            value    = 45_000,
            step     = 1000,
            help     = "Odometer reading in kilometres",
        )

        transmission = st.selectbox(
            label   = "Transmission",
            options = TRANSMISSIONS,
            index   = 1,    # default: Automatic
            help    = "Gearbox type",
        )

    with col2:
        fuel = st.selectbox(
            label   = "Fuel Type",
            options = FUELS,
            index   = 0,    # default: Petrol
        )

        body = st.selectbox(
            label   = "Body Type",
            options = BODIES,
            index   = 0,    # default: Sedan
        )

        city = st.selectbox(
            label   = "City",
            options = CITIES,
            index   = 0,    # default: Karachi
        )

    submitted = st.form_submit_button(
        label = "🔍 Predict Price Category",
        use_container_width=True,
        type="primary",
    )

# prediction
if submitted:
    payload = {
        "year"        : int(year),
        "engine"      : float(engine),
        "mileage"     : int(mileage),
        "transmission": transmission,
        "fuel"        : fuel,
        "body"        : body,
        "city"        : city if city != "Other" else "Karachi", 
    }

    with st.spinner("Sending request to model …"):
        result = call_predict_api(payload)

    if result:
        st.divider()
        st.subheader("📊 Prediction Result")

        category = result["price_category"]
        prob_high = result["probability_high"]
        prob_low  = result["probability_low"]
        median    = result["median_price_pkr"]

        if category == "High Price":
            st.success(f"## {category}")
            st.markdown(
                f"This car is likely priced above the median listing price "
                f"of **PKR {median:,.0f}**."
            )
        else:
            st.warning(f"## {category}")
            st.markdown(
                f"This car is likely priced below the median listing price "
                f"of **PKR {median:,.0f}**."
            )

        # confidence scores 
        st.markdown("**Confidence Scores:**")
        col_a, col_b = st.columns(2)
        with col_a:
            st.metric("High Price", f"{prob_high*100:.1f}%")
            st.progress(prob_high)
        with col_b:
            st.metric("Low Price", f"{prob_low*100:.1f}%")
            st.progress(prob_low)

        with st.expander("Raw API Response"):
            st.json(result)

# footer 
st.divider()

