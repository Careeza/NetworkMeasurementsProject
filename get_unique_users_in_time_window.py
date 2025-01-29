import os
import psutil
import pandas as pd
import time

def memory_usage():
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 10**6  # Convert bytes to MB

def get_unique_users_in_time_window(chunks, time_window, output_file):
	time_dic = {}
	for i, chunk in enumerate(chunks):
		if i % 1000 == 0:
			print(f"Processing chunk {i}")
		chunk['timestamp'] = pd.to_datetime(chunk['timestamp'], format='mixed')
		chunk = chunk.set_index('timestamp').resample(time_window)
		for time_interval, group in chunk:
			if time_interval not in time_dic:
				time_dic[time_interval] = dict()
			for user_id in group['user_id']:
				time_dic[time_interval][user_id] = 1

	time_series_data = pd.DataFrame({
		'timestamp': list(time_dic.keys()),
		'unique_users': [len(dic) for dic in time_dic.values()]
	})

	time_series_data.to_csv(output_file, index=False)

chunk_size = 10_000
chunks = pd.read_csv("data.csv", chunksize=chunk_size)

start_time = time.time()
get_unique_users_in_time_window(chunks, '1h', 'output.csv')
end_time = time.time()
print(f"Execution time: {end_time - start_time:.2f} seconds")
print(f"Final memory usage: {memory_usage():.2f} MB")
