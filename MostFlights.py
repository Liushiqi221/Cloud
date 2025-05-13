# MostFlights_assignment.py
import csv
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import math

def mapper(chunk):
    """
    Map phase: Count the number of flights per passenger in the given chunk.
    Returns a Counter object with passenger_id as key and flight count as value.
    """
    flight_counts = Counter()
    for record in chunk:
        passenger_id = record[0]  # First column is passenger_id
        flight_counts[passenger_id] += 1
    return flight_counts

def reducer(counter_list):
    """
    Reduce phase: Combine all Counter objects from the Map phase into a single Counter.
    """
    combined_counts = Counter()
    for counter in counter_list:
        combined_counts.update(counter)
    return combined_counts

def split_data(data, num_splits):
    """
    Split the data into num_splits parts for parallel processing.
    """
    chunk_size = math.ceil(len(data) / num_splits)
    return [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]

def process_flights():
    """
    Main function to process flight data and find the passenger with the most flights.
    """
    # Read the passenger data from CSV
    data_file = 'data/AComp_Passenger_data_no_error.csv'
    try:
        with open(data_file, newline='', encoding='utf-8') as csvfile:
            csv_reader = csv.reader(csvfile)
            # Skip header if present
            # next(csv_reader, None)
            flight_records = list(csv_reader)
    except FileNotFoundError:
        print(f"Error: File {data_file} not found.")
        return

    # Split data for parallel processing
    thread_count = 4
    data_chunks = split_data(flight_records, thread_count)

    # Map phase: Parallel processing of chunks
    with ThreadPoolExecutor(max_workers=thread_count) as executor:
        mapped_results = list(executor.map(mapper, data_chunks))

    # Reduce phase: Combine results
    final_counts = reducer(mapped_results)

    # Find passenger(s) with the highest flight count
    if final_counts:
        max_flights = max(final_counts.values())
        top_passengers = [
            (pid, count) for pid, count in final_counts.items() if count == max_flights
        ]
        for pid, count in top_passengers:
            print(f"Passenger {pid} has the highest number of flights: {count}")
    else:
        print("No passenger data found.")

if __name__ == "__main__":
    process_flights()