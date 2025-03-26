I had 2024-07-26 day. At all it was more than 20 mln. rows. 

For the basic implementation, I focused on tasks a and b, implementing them both sequentially and in parallel by dividing the data into subsets of distinct MMSI. In the task-1.py file, I limited my tests to 7 subsets and 7 workers to reduce waiting times. This resulted in approximately a 1.5x speedup in processing. (32s -> 20s for task A; 45s - > 30s)

For reading data was used chunks. When chunk size was 10 ** 6, reading time was 65 seconds, when size was 10 ** 7 , time was 56 s.

I conducted experiments for Task A and Task B, utilizing different numbers of workers while monitoring CPU and memory usage. I used 5 percent of all ships at 2024.07.26. The results are illustrated in the images labeled taska.png and taskb.png. With 7 workers, the speedup achieved for both tasks was twice.
