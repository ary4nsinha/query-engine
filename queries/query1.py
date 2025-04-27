import time

def query1(records):
    """
    Count the total number of records in the dataset.
    
    Args:
        records: Either an integer (pre-counted total) or an iterable of record dictionaries
        
    Returns:
        A formatted string with the total count
    """
    print("Executing Query 1: Count total records")
    start_time = time.time()
    
    if isinstance(records, int):
        total_count = records
        print(f"Using pre-counted total: {total_count:,} records")
    else:
        print("Counting records...")
        total_count = 0
        for _ in records:
            total_count += 1
            
            if total_count % 1000000 == 0:
                elapsed = time.time() - start_time
                print(f"Counted {total_count:,} records in {elapsed:.2f} seconds ({total_count/elapsed:.2f} records/sec)")
    
    elapsed = time.time() - start_time
    print(f"Query complete: {total_count:,} records in {elapsed:.2f} seconds")
    
    return f"total_trips: {total_count}"