import time
from collections import defaultdict

def query2(records):
    """
    Filter trips with a Trip_distance greater than 5 miles.
    Group by Payment_type and compute:
    - Count of trips per payment type
    - Average fare amount per payment type
    - Total tip amount per payment type
    
    Args:
        records: An iterable of record dictionaries
        
    Returns:
        A formatted string with the results
    """
    print("Executing Query 2: Trip Distance Filter with Payment Type Grouping")
    start_time = time.time()
    

    payment_type_stats = defaultdict(lambda: {"count": 0, "total_fare": 0.0, "total_tip": 0.0})
    
    record_count = 0
    filtered_count = 0
    
    for record in records:
        record_count += 1
        

        trip_distance = record.get('trip_distance')
        if trip_distance is not None and trip_distance > 5:
            filtered_count += 1
            

            payment_type = record.get('payment_type', 0)  
            fare_amount = record.get('fare_amount', 0.0)
            tip_amount = record.get('tip_amount', 0.0)

            stats = payment_type_stats[payment_type]
            stats["count"] += 1
            stats["total_fare"] += fare_amount
            stats["total_tip"] += tip_amount
    
    elapsed = time.time() - start_time
    print(f"Query processing complete: {record_count:,} records processed, {filtered_count:,} records matched filter")
    print(f"Processing time: {elapsed:.2f} seconds ({record_count/elapsed:.2f} records/sec)")
    
    output_lines = []
    
    for payment_type in sorted(payment_type_stats.keys()):
        stats = payment_type_stats[payment_type]
        count = stats["count"]
        
        avg_fare = stats["total_fare"] / count if count > 0 else 0
        total_tip = stats["total_tip"]
        
        output_line = f"Payment_type: {payment_type}, num_trips: {count}, avg_fare: {avg_fare:.2f}, total_tip: {total_tip:.2f}"
        output_lines.append(output_line)
    
    return "\n".join(output_lines)