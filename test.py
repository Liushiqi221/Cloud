from collections import defaultdict
import csv
import multiprocessing
import math

# Map stage
def map_stage(chunk):
    """
    Map stage: Count the number of flights per passenger in the given data chunk.
    Returns a defaultdict with passenger_id as key and flight count as value.
    """
    counts = defaultdict(int)
    for row in chunk:
        if row and len(row) > 0:  # 确保行不为空
            value = row[0]  # Assume passenger ID is in the first column
            # 避免标题行或无效数据被计入
            if value and value != "Passenger ID":
                counts[value] += 1
    return counts

# Reduce stage
def reduce_stage(mapped_counts):
    """
    Reduce stage: Combine all mapped counts into a final defaultdict.
    Aggregates flight counts for each passenger across all Map results.
    """
    final_counts = defaultdict(int)
    for counts in mapped_counts:
        for passenger_id, count in counts.items():
            final_counts[passenger_id] += count
    return final_counts

def split_data(data, num_splits):
    """
    Split the data into num_splits parts for parallel processing.
    """
    chunk_size = math.ceil(len(data) / num_splits)
    return [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]

def validate_results(original_counts):
    """
    Validate the results using AComp_Passenger_data_no_error_DateTime.csv.
    Does not print validation results, only performs the check.
    """
    validation_file = 'data/AComp_Passenger_data_no_error_DateTime.csv'
    try:
        with open(validation_file, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader, None)  # 跳过标题行（假设验证文件有标题）
            data = list(reader)
    except FileNotFoundError:
        return False  # 静默失败，不输出错误信息

    # Split data for validation
    num_workers = 4
    data_chunks = split_data(data, num_workers)

    # Map and Reduce for validation data
    pool = multiprocessing.Pool(num_workers)
    map_results = pool.map(map_stage, data_chunks)
    pool.close()
    pool.join()

    validation_counts = reduce_stage(map_results)

    # Compare results after converting to dict (no output)
    return dict(validation_counts) == dict(original_counts)

def main():
    """
    Main function: Coordinate MapReduce process to find the passenger with the most flights,
    and validate the results using AComp_Passenger_data_no_error_DateTime.csv.
    """
    # Read the passenger data from CSV
    file = 'data/AComp_Passenger_data_no_error.csv'
    try:
        with open(file, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            # 不跳过第一行，因为第一行是数据
            data = list(reader)
    except FileNotFoundError:
        return  # 静默失败，不输出错误信息

    # Split data for parallel processing
    num_workers = 4  # Set the number of parallel worker processes for Map stage
    data_chunks = split_data(data, num_workers)

    # Map stage: Parallel processing of data chunks
    pool = multiprocessing.Pool(num_workers)
    map_results = pool.map(map_stage, data_chunks)
    pool.close()
    pool.join()

    # Reduce stage: Combine results from all Map processes
    final_counts = reduce_stage(map_results)

    # Find the passenger with the most flights
    max_value = max(final_counts, key=final_counts.get)
    max_count = final_counts[max_value]
    print(f"Passenger {max_value} has the highest number of flights: {max_count}")

    # Validate the results using the DateTime file (silently)
    validate_results(final_counts)

if __name__ == "__main__":
    main()