import sys
import time
import multiprocessing
from json_parser import parse_json_file
from queries.query1 import query1
from queries.query2 import query2
from queries.query3 import query3
from queries.query4 import query4

def execute_query(query_name, file_path="taxi-trips-data.json"):
    """Execute a specific query by name with detailed timing"""
    print(f"Executing {query_name}...")

    total_start_time = time.time()

    num_processes = multiprocessing.cpu_count()
    print(f"Using {num_processes} CPU cores for processing")

    result = None

    if query_name == "query1":
        record_count = parse_json_file(file_path, query_type="query1", num_processes=num_processes)
        result = query1(record_count)

    elif query_name == "query2":
        records = parse_json_file(file_path, query_type="query2", num_processes=num_processes)
        result = query2(records)

    elif query_name == "query3":
        records = parse_json_file(file_path, query_type="query3", num_processes=num_processes)
        result = query3(records)

    elif query_name == "query4":
        records = parse_json_file(file_path, query_type="query4", num_processes=num_processes)
        result = query4(records)

    else:
        result = f"Unknown query: {query_name}"

    # Calculate timing
    total_end_time = time.time()
    total_execution_time = total_end_time - total_start_time

    # Print result (you can adjust this if you want a different format)
    print(f"\nQuery Result:")
    print(result)

    print(f"\nTotal execution time: {total_execution_time:.2f} seconds")

    if result and "Unknown query" not in result:
        sys.exit(0)  
    else:
        sys.exit(1)  


def main():
    """Main function to handle command line arguments"""
    if len(sys.argv) > 1:
        query_name = sys.argv[1]
        execute_query(query_name)
    else:
        print("Usage: python your_solution.py <query_name>")
        print("Available queries: query1, query2, query3, query4")
        sys.exit(1)  

if __name__ == "__main__":
    main()
