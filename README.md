I had 2024-07-26 day. At all it was more than 20 mln. rows. 

For the basic implementation, I focused on tasks a and b, implementing them both sequentially and in parallel by dividing the data into subsets of distinct MMSI. In the task-1.py file, I limited my tests to 7 subsets and 7 workers to reduce waiting times. This resulted in approximately a 1.5x speedup in processing. (32s -> 20s for task A; 45s - > 30s)

For reading data was used chunks. When chunk size was 10 ** 6, reading time was 65 seconds, when size was 10 ** 7 , time was 56 s.
import pandas as pd
import time33 as tw
import multiprocessing as mp
import numpy as np
import time
from tqdm import tqdm
import psutil


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



def process_groups(groups, n_workers, chunksize, task):
    """Apdoroja grupes su multiprocessing, progreso baru ir išmatuoja CPU/RAM naudojimą."""
    total_tasks = len(groups)
    results = []

    start_time = time.time()
    with mp.Pool(n_workers) as pool:
        with tqdm(total=total_tasks, desc=f"Workers: {n_workers}, Chunk: {chunksize}") as pbar:
            for _ in pool.imap_unordered(task, groups, chunksize=chunksize):
                results.append(_)
                pbar.update(1)

    end_time = time.time()
    elapsed_time = end_time - start_time

    return elapsed_time

if __name__ == "__main__":
    filename = 'C:/Users/ukniukstaite/OneDrive - Amber Grid/Documents/bandymas/aisdk-2024-07-26.csv'

    chunk_size = 10 ** 7
    chunk_list = []
    data = read_data(chunk_size)
    print(f"Data has {data.shape[0]} rows and {data.shape[1]} columns.")
    
    grouped_data = [group for _, group in data.groupby('MMSI')]  
    num_groups_to_take = max(1, int(len(grouped_data) * 0.05)) 
    print(f"taken groups: {num_groups_to_take}")

    # Testuojame skirtingus n_workers ir chunksize derinius
    for n_workers in [7]:
        for chunksize in [2, 5, 10]:
            elapsed = process_groups(grouped_data, n_workers, chunksize, task_A)
            print(f"Workers: {n_workers} | Chunk: {chunksize} | Time: {elapsed:.2f}s")

    for n_workers in [7]:
        for chunksize in [2, 5, 10]:
            elapsed = process_groups(grouped_data, n_workers, chunksize, task_B)
            print(f"Workers: {n_workers} | Chunk: {chunksize} | Time: {elapsed:.2f}s")




if __name__ == "__main__":
    groups = list(range(20))  # Tarkime, turime 20 grupių

    # Testuojame skirtingus n_workers ir chunksize derinius
    for n_workers in [1, 2, 4, 8]:
        for chunksize in [1, 2, 5, 10]:
            elapsed = process_groups(groups, n_workers, chunksize)
            print(f"Workers: {n_workers} | Chunk: {chunksize} | Time: {elapsed:.2f}s")
