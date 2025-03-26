def process_group(task, df):
    result = task(df)  # Apdorokite grupę su funkcija `task` (A ar B užduotys)
    memory_used = measure_memory(task, df)  # Pamatykite atminties naudojimą
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
                for result, memory_used in pool.imap(process_group, [(task_A, df) for df in grouped_data]):
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
                for result, memory_used in pool.imap(process_group, [(task_B, df) for df in grouped_data]):
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
