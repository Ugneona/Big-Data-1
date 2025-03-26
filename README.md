import pandas as pd
import time33 as tw
import multiprocessing as mp
import numpy as np
import time
from tqdm import tqdm
import psutil

filename = 'C:/Users/ukniukstaite/OneDrive - Amber Grid/Documents/bandymas/aisdk-2024-07-26.csv'


chunk_size = 10 ** 7
chunk_list = []


def memory_usage():
    process=psutil.Process()
    return process.memory_info().rss / (1024*1024) #MB




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
    memory_before=psutil.Process().memory_info().rss/(1024*1024)
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
    memory_after=psutil.Process().memory_info().rss/(1024*1024)
    return sub_df, (memory_after-memory_before)

@tw.timeit
def task_B(sub_df):
    memory_before=psutil.Process().memory_info().rss/(1024*1024)
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
    memory_after=psutil.Process().memory_info().rss/(1024*1024)
    return sub_df, (memory_after-memory_before)

def speed_up(time_seq, time_par):
    speed_up_variable=(time_seq/time_par)
    print(f"Speed-up was: {speed_up_variable}")
    return speed_up_variable

def many_workers(workers_list, grouped_data):
    analysis_a=[]
    analysis_b=[]
    for worker in workers_list:
        print(f"Testing with {worker} workers A task")
        cpu_before=psutil.cpu_percent(interval=None)
        start = time.perf_counter()

        with mp.Pool(worker) as pool:
            with tqdm(total=len(grouped_data), desc="Processing Groups", unit='group') as pbar:
                results_A=[]
                memory_usage_list=[]
                for result, memory_usage in pool.imap_unordered(task_A, grouped_data):
                    results_A.append(result)
                    memory_usage_list.append(memory_usage)
                    pbar.update(1)
        end = time.perf_counter()
        cpu_after=psutil.cpu_percent(interval=None)
        execution_time=end-start
        avg_cpu_usage=(cpu_before+cpu_after)/2
        memory_used=sum(memory_usage_list)
        if worker == 1:
            baseline_time=execution_time
            print(f"seq time {baseline_time}s")
        analysis_a.append((worker, execution_time, avg_cpu_usage, memory_used,speed_up(baseline_time ,execution_time) ))
        print(f"task a, workers: {worker}, time: {execution_time} s, cpu: {avg_cpu_usage}%, memory: {memory_used}MB, speed-up: {speed_up(baseline_time ,execution_time)}\n")

    for worker in workers_list:
            print(f"Testing with {worker} workers B task")
            cpu_before=psutil.cpu_percent(interval=None)
            start = time.perf_counter()

            with mp.Pool(worker) as pool:
                with tqdm(total=len(grouped_data), desc="Processing Groups", unit='group') as pbar:
                    results_b=[]
                    memory_usage_list=[]
                    for result, memory in pool.imap_unordered(task_B, grouped_data):
                        results_b.append(result)
                        pbar.update(1)
                        memory_usage_list.append(memory)
            end = time.perf_counter()
            cpu_after=psutil.cpu_percent(interval=None)
            execution_time=end-start
            avg_cpu_usage=(cpu_before+cpu_after)/2
            memory_used=sum(memory_usage_list)
            if worker == 1:
                baseline_time=execution_time
                print(f"seq time {baseline_time}s")
            analysis_a.append((worker, execution_time, avg_cpu_usage, memory_used,speed_up(baseline_time ,execution_time) ))
            print(f"task b, workers: {worker}, time: {execution_time} s, cpu: {avg_cpu_usage}%, memory: {memory_used}MB, speed-up: {speed_up(baseline_time ,execution_time)}\n")
        
    return results_A,  results_b, analysis_a, analysis_b

# def calculate_distance_matrix(latitudes, longitudes):
#     """
#     Apskaičiuoja atstumų matricą tarp visų laivų.
#     """
#     R = 6371  # Žemės spindulys kilometrais
#     lat_rad = np.radians(latitudes)
#     lon_rad = np.radians(longitudes)

#     lat_diff = lat_rad[:, None] - lat_rad[None, :]
#     lon_diff = lon_rad[:, None] - lon_rad[None, :]
    
#     a = np.sin(lat_diff / 2) ** 2 + np.cos(lat_rad[:, None]) * np.cos(lat_rad[None, :]) * np.sin(lon_diff / 2) ** 2
#     c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    
#     return (R * c) / 1.852  # Konvertuojame į jūrmyles

# def assign_location_groups(df, distance_threshold_nm=10):
#     """
#     Priskiria laivams grupės numerį pagal jų artumą vienas kitam,
#     nenaudojant `combinations`.
#     """
#     df = df.copy()
#     df["location_group"] = -1  # Pradžioje visi laivai neturi grupės
#     group_id = 0

#     latitudes = df["Latitude"].to_numpy()
#     longitudes = df["Longitude"].to_numpy()

#     distance_matrix = calculate_distance_matrix(latitudes, longitudes)  # Atstumų matrica

#     assigned = np.full(len(df), False)  # Sekame, kurie laivai jau priskirti grupei

#     for i in range(len(df)):
#         if not assigned[i]:  # Jei laivas dar neturi grupės
#             mask = distance_matrix[i] < distance_threshold_nm  # Randame artimus laivus
#             df.loc[mask, "location_group"] = group_id  # Priskiriame tą pačią grupę
#             assigned[mask] = True  # Pažymime, kad laivai jau priskirti
#             group_id += 1

#     return df

# @tw.timeit
# def task_C(sub_df):
#     sub_df = sub_df.copy()
#     grouped_data = [group for _, group in sub_df.groupby('# Timestamp')] 
#     group1=grouped_data[0]
#     group2=grouped_data[1]

#     if len(group1) > 2:  
#         sub_df["spoofing_c"] = False
        
#         group1 = assign_location_groups(group1)

#         if group1["location_group"].any() != -1:
#             for group, _ in range(len(group1["location_group"].unique()):
#                 for mmsi in len(group1[group1['location_group'] == group]['MMSI'].unique()):
#                     lat0, lon0 = group1.iloc[group1['location_group' == group]]['Latitude'], group1.iloc[group1['location_group' == group]]['Longitude']
#                     lat1, lon1 = group2.iloc[group1['location_group' == group]]['Latitude'], group2.iloc[i]['Longitude']


#         for i in range(1, len(sub_df)):  
#             sog0, sog1, sog2 = sub_df.iloc[i - 1]['SOG'], sub_df.iloc[i]['SOG'], sub_df.iloc[i + 1]['SOG']
#             cog0, cog1, cog2 = sub_df.iloc[i - 1]['COG'], sub_df.iloc[i]['COG'], sub_df.iloc[i + 1]['COG']

#             lat0, lon0 = sub_df.iloc[i - 1]['Latitude'], sub_df.iloc[i - 1]['Longitude']
#             lat1, lon1 = sub_df.iloc[i]['Latitude'], sub_df.iloc[i]['Longitude']
#             lat2, lon2 = sub_df.iloc[i + 1]['Latitude'], sub_df.iloc[i + 1]['Longitude']

           
            
#             distance_nm1 = calculate_distance(lat1, lon1, lat2, lon2) / 1.852
#             time_diff0 = (sub_df.iloc[i]['# Timestamp'] - sub_df.iloc[i - 1]['# Timestamp']).total_seconds() / 60
#             time_diff1 = (sub_df.iloc[i+1]['# Timestamp'] - sub_df.iloc[i]['# Timestamp']).total_seconds() / 60

#             if time_diff0 > 0 and time_diff1 > 0:
#                     if ((abs(sog0 - sog1) > 10 and abs(sog1 - sog2) > 10) or
#                         (abs(cog0 - cog1) > 90 and abs(cog2 - cog1) > 90) or
#                         (distance_nm0 > 10 and distance_nm1 > 10)):  
#                         sub_df.at[sub_df.index[i], 'spoofing_c'] = True
#     else:
#         sub_df["spoofing_c"] = None

#     return sub_df


if __name__ == "__main__":
    data = read_data(chunk_size)
    print(f"Data has {data.shape[0]} rows and {data.shape[1]} columns.")
    
    grouped_data = [group for _, group in data.groupby('MMSI')]  # Skirstome duomenis pagal MMSI
    num_groups_to_take = max(1, int(len(grouped_data) * 0.05)) # Ensure at least one group is taken
    print(f"taken groups: {num_groups_to_take}")
# Take the first 10% of the groups
    selected_groups = grouped_data[:num_groups_to_take]
    results_a, results_b, analysis_a, analysis_b=many_workers([1, 2, 4, 7], selected_groups)
    print("task A")
    print("worker count | execution time (s), avg cpu usage (%) | memory used (MB)")
    for worker, time_taken, cpu_usage, memory in results_a:
        print(f"{worker:<12} | {time_taken:<18} | {cpu_usage:<17} | {memory:<13}")

    print("task B")
    print("worker count | execution time (s), avg cpu usage (%) | memory used (MB)")
    for worker, time_taken, cpu_usage, memory in results_b:
        print(f"{worker:<12} | {time_taken:<18} | {cpu_usage:<17} | {memory:<13}")
    # **Sekvencinis vykdymas**
    # start = time.perf_counter()
    # result_a = [task_A(group) for group in grouped_data[]]
    # end = time.perf_counter()
    # print(f"Sequential execution time for task_A: {end - start:.2f} seconds")

    # start = time.perf_counter()
    # cpu=1

    # with mp.Pool(cpu) as pool:
    #     with tqdm(total=len(grouped_data), desc="Processing Groups", unit='group') as pbar:
    #         results=[]
    #         for result in pool.imap_unordered(task_A, grouped_data):
    #             results.append(result)
    #             pbar.update(1)
    # end = time.perf_counter()
    # print(f"Parallel execution time for task_A with {cpu} workers: {end - start:.2f} seconds")

    # final_a=pd.concat(results, ignore_index=True)
    # grouped_data = [group for _, group in final_a.groupby('MMSI')] 

    # start = time.perf_counter()
    # cpu=1
    # with mp.Pool(cpu) as pool:
    #     with tqdm(total=len(grouped_data), desc="Processing Groups", unit='group') as pbar:
    #         results=[]
    #         for result in pool.imap_unordered(task_B, grouped_data):
    #             results.append(result)
    #             pbar.update(1)
    # end = time.perf_counter()
    # print(f"Parallel execution time for task_B with {cpu} workers: {end - start:.2f} seconds")


    # # **Paralelinis vykdymas**
    # start = time.perf_counter()
    # cpu=1
    # with mp.Pool(cpu) as pool:
    #     results_a_parallel = pool.map(task_A, grouped_data)  # Vietoj MMSI siunčiame jau atskirtus duomenis
    # end = time.perf_counter()
    # print(f"Parallel execution time for task_A with {cpu} CPU: {end - start:.2f} seconds")

    # # **Sekvencinis vykdymas**
    # # start = time.perf_counter()
    # # result_a = [task_B(group) for group in grouped_data[]]
    # # end = time.perf_counter()
    # # print(f"Sequential execution time for task_B: {end - start:.2f} seconds")

    # # **Paralelinis vykdymas**
    # start = time.perf_counter()
    # with mp.Pool(cpu) as pool:
    #     results_a_parallel = pool.map(task_B, grouped_data)  # Vietoj MMSI siunčiame jau atskirtus duomenis
    # end = time.perf_counter()
    # print(f"Parallel execution time for task_B with {cpu} CPU: {end - start:.2f} seconds")

