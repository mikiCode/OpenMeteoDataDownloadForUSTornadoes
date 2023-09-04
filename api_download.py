import pandas as pd
import time
import requests
import json
import os
import glob


df = pd.read_csv(
    r"C:\Users\Miko\Desktop\tornadoes\1950-2021_actual_tornadoes.csv", sep=","
)
df = df.drop(columns=["om", "stn", "sg"])
df = df.loc[df["yr"] > 1958]
df = df[df["tz"] == 3]
df = df.sort_values("date")
df = df.reset_index(drop=True)
df["id"] = range(len(df))
df = df[["id"] + [col for col in df.columns if col not in ["id"]]]
df.to_csv(
    r"C:\Users\Miko\Desktop\tornadoes\1959-2021_actual_tornadoesv3.csv", index=False
)

date = df.date
year = df.yr
month = df.m
day = df.dy
lat = df.slat
lon = df.slon
time = df.time.str[:2]


def downloader(latitude, longitude, date):
    response = requests.get(
        f"https://archive-api.open-meteo.com/v1/archive?latitude={latitude}&longitude={longitude}\
        &start_date={date}&end_date={date}&hourly=temperature_2m,relativehumidity_2m,dewpoint_2m,\
        apparent_temperature,pressure_msl,surface_pressure,precipitation,rain,snowfall,\
        cloudcover_low,cloudcover_mid,cloudcover_high,shortwave_radiation,direct_radiation,\
        diffuse_radiation,direct_normal_irradiance,windspeed_10m,windspeed_100m,winddirection_10m,\
        winddirection_100m,windgusts_10m,et0_fao_evapotranspiration,vapor_pressure_deficit,\
        soil_temperature_0_to_7cm,soil_temperature_7_to_28cm,soil_temperature_28_to_100cm,\
        soil_temperature_100_to_255cm,soil_moisture_0_to_7cm,soil_moisture_7_to_28cm,\
        soil_moisture_28_to_100cm,soil_moisture_100_to_255cm&models=best_match&timezone=America%2FChicago"
    )
    return response.json()


def get_nth_value(data_dict, n):
    result_dict = {}
    for key, values in data_dict.items():
        if n < len(values):
            result_dict[key] = values[n]
    return result_dict


def get_index_api(search_value):
    hour_order_api = {
        0: "00:00",
        1: "23:00",
        2: "22:00",
        3: "21:00",
        4: "20:00",
        5: "19:00",
        6: "18:00",
        7: "17:00",
        8: "16:00",
        9: "15:00",
        10: "14:00",
        11: "13:00",
        12: "12:00",
        13: "11:00",
        14: "10:00",
        15: "09:00",
        16: "08:00",
        17: "07:00",
        18: "06:00",
        19: "05:00",
        20: "04:00",
        21: "03:00",
        22: "02:00",
        23: "01:00",
    }
    for key, value in hour_order_api.items():
        if value[:2] == search_value:
            return key
    return None


def append_dict_to_file(filename, new_dict):
    # Read existing data from the file
    try:
        with open(filename, "r") as f:
            data = json.load(f)
    except FileNotFoundError:
        data = []

    # Check if the new dictionary already exists in the file
    if new_dict not in data:
        # Add the new dictionary to the list
        data.append(new_dict)
        # Write the updated data back to the file
        with open(filename, "w") as f:
            json.dump(data, f)
    return None


def open_meteo_download():
    json_files = sorted(
        glob.glob("open_meteo_tornado_*.json")
    )  # Get all existing JSON files
    if json_files:  # If there are any files
        last_file = json_files[-1]  # Get the last file
        last_id_in_file = int(
            last_file.split("-")[1].split(".")[0]
        )  # Get the last ID in this file
        data_count = last_id_in_file + 1  # Start from the next ID
    else:
        data_count = 0  # If there are no files, start from 0

    data = []  # Initialize data list
    ending_index = df.index.max()  # Get the maximum index
    start_time = time.time()  # Capture the start time

    # Start the loop from the last processed ID
    for index, row in df.loc[data_count:].iterrows():
        try:
            x = downloader(row["slat"], row["slon"], row["date"])["hourly"]
            get_index_api(row["time"][:2])
            output_data = {
                "id": row["id"],
                "data": get_nth_value(x, get_index_api(row["time"][:2])),
            }
            data.append(output_data)

            if (
                len(data) % 1000 == 0 or row["id"] == ending_index
            ):  # Save after every 1000 dictionaries or at the end
                with open(
                    f"open_meteo_tornado_{data[0]['id']}-{data[-1]['id']}.json", "w"
                ) as f:
                    json.dump(data, f)
                end_time = time.time()  # Capture the end time
                print(
                    f"Saved data from ID {data[0]['id']} to {data[-1]['id']}. \
                    This batch took {end_time - start_time} seconds to download."
                )
                start_time = time.time()  # Reset the start time
                data = []  # Reset the data list for the next 1000 dictionaries

            if row["id"] == ending_index:
                break
        except KeyboardInterrupt:
            with open(
                f"open_meteo_tornado_{data[0]['id']}-{data[-1]['id']}.json", "w"
            ) as f:
                json.dump(data, f)
            print(
                f"Interrupted. Saved progress from ID {data[0]['id']} to {data[-1]['id']}."
            )
            break  # Exit the loop


open_meteo_download()


def merge_json_files_from_dir(directory, output_file):
    data = []
    for filename in os.listdir(directory):
        if filename.endswith(".json"):
            filepath = os.path.join(directory, filename)
            with open(filepath, "r") as f:
                json_data = json.load(f)
                data += json_data
    with open(output_file, "w") as f:
        json.dump(data, f)


merge_json_files_from_dir(
    "C:/Users/Miko/Desktop/tornadoes/merge", "merged_weather.json"
)
