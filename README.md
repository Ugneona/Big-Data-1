I had 2024-07-26 day. At all it was more than 20 mln. rows. 

For the basic implementation, I focused on tasks a and b, implementing them both sequentially and in parallel by dividing the data into subsets of distinct MMSI. In the task-1.py file, I limited my tests to 7 subsets and 7 workers to reduce waiting times. This resulted in approximately a 1.5x speedup in processing. (32s -> 20s for task A; 45s - > 30s)

For reading data was used chunks. When chunk size was 10 ** 6, reading time was 65 seconds, when size was 10 ** 7 , time was 56 s.
def process_groups(groups, n_workers, chunksize):
    """Apdoroja grupes su multiprocessing, progreso baru ir išmatuoja CPU/RAM naudojimą."""
    total_tasks = len(groups)
    results = []

    start_time = time.time()
    with mp.Pool(n_workers) as pool:
        with tqdm(total=total_tasks, desc=f"Workers: {n_workers}, Chunk: {chunksize}") as pbar:
            for _ in pool.imap_unordered(task_A, groups, chunksize=chunksize):
                results.append(_)
                pbar.update(1)

    end_time = time.time()
    elapsed_time = end_time - start_time

    return elapsed_time

if __name__ == "__main__":
    groups = list(range(20))  # Tarkime, turime 20 grupių

    # Testuojame skirtingus n_workers ir chunksize derinius
    for n_workers in [1, 2, 4, 8]:
        for chunksize in [1, 2, 5, 10]:
            elapsed = process_groups(groups, n_workers, chunksize)
            print(f"Workers: {n_workers} | Chunk: {chunksize} | Time: {elapsed:.2f}s")
