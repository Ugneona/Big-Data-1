import pandas as pd
import time_wrapper as tw
import multiprocessing as mp
import numpy as np
import time

filename = 'C:/Users/ugneo/OneDrive/Stalinis kompiuteris/Duomenų mokslas/magistras__kursas/2 semestras/big data/1-task/aisdk-2024-07-26.csv'
chunk_size = 10 ** 6
chunk_list = []

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

if __name__ == "__main__":
    data = read_data(chunk_size)
    print(f"Data has {data.shape[0]} rows and {data.shape[1]} columns.")
    
    grouped_data = [group for _, group in data.groupby('MMSI')]  # Skirstome duomenis pagal MMSI

    # **Sekvencinis vykdymas**
    start = time.perf_counter()
    result_a = [task_A(group) for group in grouped_data[:5]]
    end = time.perf_counter()
    print(f"Sequential execution time: {end - start:.2f} seconds")

    # **Paralelinis vykdymas**
    start = time.perf_counter()
    with mp.Pool(mp.cpu_count()) as pool:
        results_a_parallel = pool.map(task_A, grouped_data[:10])  # Vietoj MMSI siunčiame jau atskirtus duomenis
    end = time.perf_counter()
    print(f"Parallel execution time: {end - start:.2f} seconds")
