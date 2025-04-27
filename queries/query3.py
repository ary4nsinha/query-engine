import time
from collections import defaultdict

def is_january_2024(date_str):
    """Check if date string is in January 2024 (YYYY-MM-DD format)"""
    if not date_str or len(date_str) < 10:
        return False
    
    year_str = date_str[0:4]
    month_str = date_str[5:7]
    
    return year_str == '2024' and month_str == '01'

def query3(records):
    """
    Filter trips with:
    - Store_and_fwd_flag equal to 'Y' (store and forward trips)
    - Pickup date in January 2024 (between '2024-01-01' and '2024-01-31' inclusive)
    
    Group by VendorID and compute:
    - Count of trips per vendor
    - Average passenger count per vendor
    
    Args:
        records: An iterable of record dictionaries
        
    Returns:
        A formatted string with the results
    """
    print("Executing Query 3: Store-and-Forward Flag and Date Filter with Vendor Grouping")
    start_time = time.time()
    
    vendor_stats = defaultdict(lambda: {"count": 0, "passenger_sum": 0})
    
    record_count = 0
    filtered_count = 0
    
    for record in records:
        record_count += 1
        
        store_fwd_flag = record.get('store_and_fwd_flag')
        pickup_datetime = record.get('tpep_pickup_datetime')
        
        if (store_fwd_flag == 'Y' and pickup_datetime and is_january_2024(pickup_datetime)):
            filtered_count += 1
            
            vendor_id = record.get('VendorID')
            if vendor_id is None:
                continue  
                
            passenger_count = record.get('passenger_count', 0)
            
            stats = vendor_stats[vendor_id]
            stats["count"] += 1
            stats["passenger_sum"] += passenger_count
    
    elapsed = time.time() - start_time
    print(f"Query processing complete: {record_count:,} records processed, {filtered_count:,} records matched filter")
    print(f"Processing time: {elapsed:.2f} seconds ({record_count/elapsed:.2f} records/sec)")
    
    output_lines = []
    
    for vendor_id in sorted(vendor_stats.keys()):
        stats = vendor_stats[vendor_id]
        count = stats["count"]
        
        avg_passengers = stats["passenger_sum"] / count if count > 0 else 0
        
        output_line = f"VendorID: {vendor_id}, trips: {count}, avg_passengers: {avg_passengers:.2f}"
        output_lines.append(output_line)
    
    return "\n".join(output_lines)