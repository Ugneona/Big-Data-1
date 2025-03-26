ion read_data took 48.26s
Data has 20697678 rows and 26 columns.
taken groups: 362
Testing with 1 workers A task
Processing Groups:   0%|                                                                                                         | 0/362 [00:00<?, ?group/s]
Traceback (most recent call last):
  File "c:/Users/ukniukstaite/OneDrive - Amber Grid/Documents/bandymas/import pandas as pd.py", line 256, in <module>
    results_a, results_b, analysis_a, analysis_b=many_workers([1, 2, 4, 7], selected_groups)
  File "c:/Users/ukniukstaite/OneDrive - Amber Grid/Documents/bandymas/import pandas as pd.py", line 110, in many_workers
    for result, memory_used in pool.imap(lambda df: measure_memory(task_A, df), grouped_data):
  File "C:\Program Files\WindowsApps\PythonSoftwareFoundation.Python.3.8_3.8.2800.0_x64__qbz5n2kfra8p0\lib\multiprocessing\pool.py", line 868, in next       
    raise value
  File "C:\Program Files\WindowsApps\PythonSoftwareFoundation.Python.3.8_3.8.2800.0_x64__qbz5n2kfra8p0\lib\multiprocessing\pool.py", line 537, in _handle_tasks
    put(task)
  File "C:\Program Files\WindowsApps\PythonSoftwareFoundation.Python.3.8_3.8.2800.0_x64__qbz5n2kfra8p0\lib\multiprocessing\connection.py", line 206, in send 
    self._send_bytes(_ForkingPickler.dumps(obj))
  File "C:\Program Files\WindowsApps\PythonSoftwareFoundation.Python.3.8_3.8.2800.0_x64__qbz5n2kfra8p0\lib\multiprocessing\reduction.py", line 51, in dumps  
    cls(buf, protocol).dump(obj)
AttributeError: Can't pickle local object 'many_workers.<locals>.<lambda>'
def memory_usage():
    """Returns current process memory usage in MB."""
    process = psutil.Process()
    return process.memory_info().rss / (1024 * 1024)


def calculate_distance(lat1, lon1, lat2, lon2):
    """Calculates Haversine distance between two latitude/longitude points."""
    R = 6371  
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    return R * c


def measure_memory(func, *args):
    """Measures memory usage of a function."""
    memory_before = memory_usage()
    result = func(*args)
    memory_after = memory_usage()
    memory_used = memory_after - memory_before
    return result, memory_used

def many_workers(workers_list, grouped_data):
    analysis_a = []
    analysis_b = []

    for worker in workers_list:
        print(f"Testing with {worker} workers A task")
        start = time.perf_counter()
        cpu_before = psutil.cpu_percent(interval=None)

        with mp.Pool(worker) as pool:
            with tqdm(total=len(grouped_data), desc="Processing Groups", unit='group') as pbar:
                results_A = []
                memory_usage_list = []
                for result, memory_used in pool.imap(lambda df: measure_memory(task_A, df), grouped_data):
                    results_A.append(result)
                    memory_usage_list.append(memory_used)
                    pbar.update(1)

        end = time.perf_counter()
        cpu_after = psutil.cpu_percent(interval=None)
        execution_time = end - start
        avg_cpu_usage = (cpu_before + cpu_after) / 2
        memory_used = sum(memory_usage_list)

        if worker == 1:
            baseline_time = execution_time

        analysis_a.append((worker, execution_time, avg_cpu_usage, memory_used))
        print(f"Task A, workers: {worker}, time: {execution_time}s, CPU: {avg_cpu_usage}%, Memory: {memory_used}MB")

    for worker in workers_list:
        print(f"Testing with {worker} workers B task")
        start = time.perf_counter()
        cpu_before = psutil.cpu_percent(interval=None)

        with mp.Pool(worker) as pool:
            with tqdm(total=len(grouped_data), desc="Processing Groups", unit='group') as pbar:
                results_B = []
                memory_usage_list = []
                for result, memory_used in pool.imap(lambda df: measure_memory(task_B, df), grouped_data):
                    results_B.append(result)
                    memory_usage_list.append(memory_used)
                    pbar.update(1)

        end = time.perf_counter()
        cpu_after = psutil.cpu_percent(interval=None)
        execution_time = end - start
        avg_cpu_usage = (cpu_before + cpu_after) / 2
        memory_used = sum(memory_usage_list)

        if worker == 1:
            baseline_time = execution_time

        analysis_b.append((worker, execution_time, avg_cpu_usage, memory_used))
        print(f"Task B, workers: {worker}, time: {execution_time}s, CPU: {avg_cpu_usage}%, Memory: {memory_used}MB")

    return results_A, results_B, analysis_a, analysis_b
