import os
import psutil
import pandas as pd
import time
from datasketch import HyperLogLog


def memory_usage():
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 10**6  # Convert bytes to MB

def estimate_unique_users_in_time_window(chunks, time_window, output_file):
	time_hll = {}
	for i, chunk in enumerate(chunks):
		if i % 1000 == 0:
			print(f"Processing chunk {i}")
		chunk['timestamp'] = pd.to_datetime(chunk['timestamp'], format='mixed')
		chunk = chunk.set_index('timestamp').resample(time_window)
		for time_interval, group in chunk:
			if time_interval not in time_hll:
				time_hll[time_interval] = HyperLogLog()
			for user_id in group['user_id']:
				time_hll[time_interval].update(str(user_id).encode('utf-8'))

	time_series_data = pd.DataFrame({
		'timestamp': list(time_hll.keys()),
		'unique_users': [hll.count() for hll in time_hll.values()]
	})

	time_series_data.to_csv(output_file, index=False)
	
chunk_size = 10_000
chunks = pd.read_csv("data.csv", chunksize=chunk_size)

start_time = time.time()
estimate_unique_users_in_time_window(chunks, '1h', 'output2.csv')
end_time = time.time()
print(f"Execution time: {end_time - start_time:.2f} seconds")
print(f"Final memory usage: {memory_usage():.2f} MB")