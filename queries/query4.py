import time
from collections import defaultdict

def extract_date(datetime_str):
    """Extract date part from datetime string (YYYY-MM-DD format)"""
    if not datetime_str or len(datetime_str) < 10:
        return None
    return datetime_str[0:10]

def is_january_2024(date_str):
    """Check if date string is in January 2024 (YYYY-MM-DD format)"""
    if not date_str or len(date_str) < 10:
        return False
    
    year_str = date_str[0:4]
    month_str = date_str[5:7]
    
    return year_str == '2024' and month_str == '01'

def query4(records):
    """
    Extract the date from tpep_pickup_datetime and for January 2024:
    - Group trips by day
    - For each day, compute:
      - Total number of trips
      - Average number of passengers
      - Average trip distance
      - Average fare amount
      - Total tip amount
    """
    print("Executing Query 4: Daily Statistics for January 2024")
    start_time = time.time()
    
    daily_stats = defaultdict(lambda: {
        "count": 0,
        "passenger_sum": 0,
        "distance_sum": 0.0,
        "fare_sum": 0.0,
        "tip_sum": 0.0
    })
    
    record_count = 0
    filtered_count = 0
    
    for record in records:
        record_count += 1
        
        pickup_datetime = record.get('tpep_pickup_datetime')
        if not pickup_datetime:
            continue
        
        date = extract_date(pickup_datetime)
        
        if is_january_2024(date):
            filtered_count += 1
            
            # SAFE extraction
            passenger_count = record.get('passenger_count') or 0
            trip_distance = record.get('trip_distance') or 0.0
            fare_amount = record.get('fare_amount') or 0.0
            tip_amount = record.get('tip_amount') or 0.0
            
            stats = daily_stats[date]
            stats["count"] += 1
            stats["passenger_sum"] += passenger_count
            stats["distance_sum"] += trip_distance
            stats["fare_sum"] += fare_amount
            stats["tip_sum"] += tip_amount
    
    elapsed = time.time() - start_time
    print(f"Query processing complete: {record_count:,} records processed, {filtered_count:,} records matched filter")
    print(f"Processing time: {elapsed:.2f} seconds ({record_count/elapsed:.2f} records/sec)")
    
    output_lines = []
    
    for date in sorted(daily_stats.keys()):
        stats = daily_stats[date]
        count = stats["count"]
        
        if count > 0:
            avg_passengers = stats["passenger_sum"] / count
            avg_distance = stats["distance_sum"] / count
            avg_fare = stats["fare_sum"] / count
            total_tip = stats["tip_sum"]
            
            output_line = (
                f"trip_date: {date}, "
                f"total_trips: {count}, "
                f"avg_passengers: {avg_passengers:.2f}, "
                f"avg_distance: {avg_distance:.2f}, "
                f"avg_fare: {avg_fare:.2f}, "
                f"total_tip: {total_tip:.2f}"
            )
            output_lines.append(output_line)
    
    return "\n".join(output_lines)
