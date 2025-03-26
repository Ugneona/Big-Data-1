#This file compares the execution times of tasks A and B among sequential and parallel task execution.

import pandas as pd
import time_wrapper as tw
import multiprocessing as mp
import numpy as np
import time



@tw.timeit
def read_data(chunk_size):
    print(f"Chunk size: {chunk_size}")
    for chunk in pd.read_csv(filename, chunksize=chunk_size, parse_dates=['# Timestamp']):
        chunk_list.append(chunk)
    return pd.concat(chunk_list, ignore_index=True)

def calculate_distance(lat1, lon1, lat2, lon2):
    R = 6371
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    return R * c

@tw.timeit
def task_A(sub_df):
    sub_df = sub_df.sort_values(by='# Timestamp').copy()
    
    if len(sub_df) > 1:
        sub_df["spoofing_a"] = False
        for i in range(1, len(sub_df)):
            lat1, lon1 = sub_df.iloc[i - 1]['Latitude'], sub_df.iloc[i - 1]['Longitude']
            lat2, lon2 = sub_df.iloc[i]['Latitude'], sub_df.iloc[i]['Longitude']
            distance_km = calculate_distance(lat1, lon1, lat2, lon2)
            distance_nm = distance_km / 1.852  
            time_diff = (sub_df.iloc[i]['# Timestamp'] - sub_df.iloc[i - 1]['# Timestamp']).total_seconds() / 60  
            if time_diff > 0 and (distance_nm / time_diff) > 50:  
                sub_df.at[sub_df.index[i], 'spoofing_a'] = True
    else:
        sub_df["spoofing_a"] = None
    return sub_df

@tw.timeit
def task_B(sub_df):
    sub_df = sub_df.sort_values(by='# Timestamp').copy()

    if len(sub_df) > 2:  
        sub_df["spoofing_b"] = False

        for i in range(1, len(sub_df) - 2):  
            sog0, sog1, sog2 = sub_df.iloc[i - 1]['SOG'], sub_df.iloc[i]['SOG'], sub_df.iloc[i + 1]['SOG']
            cog0, cog1, cog2 = sub_df.iloc[i - 1]['COG'], sub_df.iloc[i]['COG'], sub_df.iloc[i + 1]['COG']

            lat0, lon0 = sub_df.iloc[i - 1]['Latitude'], sub_df.iloc[i - 1]['Longitude']
            lat1, lon1 = sub_df.iloc[i]['Latitude'], sub_df.iloc[i]['Longitude']
            lat2, lon2 = sub_df.iloc[i + 1]['Latitude'], sub_df.iloc[i + 1]['Longitude']

           
            distance_nm0 = calculate_distance(lat0, lon0, lat1, lon1) / 1.852
            distance_nm1 = calculate_distance(lat1, lon1, lat2, lon2) / 1.852
            time_diff0 = (sub_df.iloc[i]['# Timestamp'] - sub_df.iloc[i - 1]['# Timestamp']).total_seconds() / 60
            time_diff1 = (sub_df.iloc[i+1]['# Timestamp'] - sub_df.iloc[i]['# Timestamp']).total_seconds() / 60

            if time_diff0 > 0 and time_diff1 > 0:
                    if ((abs(sog0 - sog1) > 10 and abs(sog1 - sog2) > 10) or
                        (abs(cog0 - cog1) > 90 and abs(cog2 - cog1) > 90) or
                        (distance_nm0 > 10 and distance_nm1 > 10)):  
                        sub_df.at[sub_df.index[i], 'spoofing_b'] = True
    else:
        sub_df["spoofing_b"] = None

    return sub_df

if __name__ == "__main__":

    filename = 'C:/Users/ugneo/OneDrive/Stalinis kompiuteris/Duomenų mokslas/magistras__kursas/2 semestras/big data/1-task/aisdk-2024-07-26.csv'
    chunk_size = 10 ** 6
    chunk_list = []
    data = read_data(chunk_size)
    print(f"Data has {data.shape[0]} rows and {data.shape[1]} columns.")
    
    grouped_data = [group for _, group in data.groupby('MMSI')]  # Skirstome duomenis pagal MMSI

    # **Sekvencinis vykdymas**
    start = time.perf_counter()
    result_a = [task_A(group) for group in grouped_data[:7]]
    end = time.perf_counter()
    print(f"Sequential execution time for task A: {end - start:.2f} seconds")

    # **Paralelinis vykdymas**
    start = time.perf_counter()
    with mp.Pool(mp.cpu_count()-1) as pool:
        results_a_parallel = pool.map(task_A, grouped_data[:7]) \
    end = time.perf_counter()
    print(f"Parallel execution time for task A: {end - start:.2f} seconds")


    # **Sekvencinis vykdymas**
    start = time.perf_counter()
    result_b = [task_B(group) for group in grouped_data[:7]] #norint visiems laivams (MMSI) reiketu vietoj grouped_data[:7] naudoti grouped_data
    end = time.perf_counter()
    print(f"Sequential execution time for task B: {end - start:.2f} seconds")

    # **Paralelinis vykdymas**
    start = time.perf_counter()
    with mp.Pool(mp.cpu_count()-1) as pool:
        results_a_parallel = pool.map(task_B, grouped_data[:7]) 
    end = time.perf_counter()
    print(f"Parallel execution time for task B: {end - start:.2f} seconds")
