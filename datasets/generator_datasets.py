import csv
import random
from datetime import datetime, timedelta


# ==============================
# CONFIGURACIÓN
# ==============================

NUM_RECORDS = 5000
NUM_RIGS = 10
NUM_SENSORS = 20
ANOMALY_PROBABILITY = 0.05  # 5% eventos anómalos

OUTPUT_FILE = "sensor_data.csv"


# ==============================
# DEFINICIÓN DE RIGS Y SENSORES
# ==============================

rigs = [f"RIG_{str(i).zfill(2)}" for i in range(1, NUM_RIGS + 1)]
sensors = [f"SEN_{str(i).zfill(2)}" for i in range(1, NUM_SENSORS + 1)]

sensor_types_map = {
    "temperature": {"unit": "C", "base": 80, "variance": 10},
    "vibration": {"unit": "mm/s", "base": 3, "variance": 2},
    "pressure": {"unit": "psi", "base": 2500, "variance": 500},
    "flow": {"unit": "l/min", "base": 110, "variance": 20},
}

sensor_type_list = list(sensor_types_map.keys())


# ==============================
# GENERACIÓN DEL CSV
# ==============================

def generate_sensor_data():
    start_time = datetime(2025, 2, 1, 0, 0, 0)

    with open(OUTPUT_FILE, mode="w", newline="") as file:
        writer = csv.writer(file)

        # Header
        writer.writerow([
            "event_id",
            "rig_id",
            "sensor_id",
            "sensor_type",
            "value",
            "unit",
            "event_timestamp"
        ])

        for i in range(NUM_RECORDS):

            rig = random.choice(rigs)
            sensor = random.choice(sensors)
            sensor_type = random.choice(sensor_type_list)

            config = sensor_types_map[sensor_type]

            # Simulación de degradación progresiva
            degradation_factor = (i / NUM_RECORDS) * config["variance"]

            value = random.uniform(
                config["base"] - config["variance"],
                config["base"] + config["variance"]
            ) + degradation_factor

            # Inyección de anomalías
            if random.random() < ANOMALY_PROBABILITY:
                value *= random.uniform(1.5, 2.5)

            timestamp = start_time + timedelta(seconds=i * 30)

            writer.writerow([
                f"E{str(i + 1).zfill(6)}",
                rig,
                sensor,
                sensor_type,
                round(value, 2),
                config["unit"],
                timestamp.strftime("%Y-%m-%d %H:%M:%S")
            ])

    print(f"Archivo generado correctamente: {OUTPUT_FILE}")


# ==============================
# EJECUCIÓN
# ==============================

if __name__ == "__main__":
    generate_sensor_data()