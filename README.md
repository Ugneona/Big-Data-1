
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
