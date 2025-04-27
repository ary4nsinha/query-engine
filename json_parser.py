import time
import multiprocessing
import os
import traceback
from multiprocessing import Pool, Manager

class CustomJsonParser:
    def __init__(self, filepath):
        self.filepath = filepath
        if filepath:
            self.file_size = os.path.getsize(filepath)
        
        self.expected_fields = [
            ('tpep_pickup_datetime', str),
            ('tpep_dropoff_datetime', str),
            ('VendorID', int),
            ('passenger_count', int),
            ('trip_distance', float),
            ('payment_type', int),
            ('fare_amount', float),
            ('tip_amount', float),
            ('store_and_fwd_flag', str)
        ]
        
        self.field_markers = [f'"{field_name}"' for field_name, _ in self.expected_fields]
    
    def parse_file(self, query_type=None, num_processes=None):
        """
        Parse the file and return appropriate results based on query type
        
        Args:
            query_type: The type of query being executed (for optimizations)
            num_processes: Number of processes to use for parsing
        
        Returns:
            For query1: Returns the total count directly
            For other queries: Returns a generator of records
        """
        if num_processes is None:
            num_processes = min(multiprocessing.cpu_count(), 16)  
        
        print(f"Starting JSON parsing with {num_processes} processes...")
        start_time = time.time()
        

        if query_type == "query1":
            count = self._count_records_parallel(num_processes)
            
            end_time = time.time()
            elapsed = end_time - start_time
            print(f"Parsing completed: {count:,} records in {elapsed:.2f} seconds")
            print(f"Average speed: {count/elapsed:.2f} records/second")
            
            return count
        else:
            return self._parse_records_generator(num_processes)
    
    def _count_records_parallel(self, num_processes):
        """
        Count records in parallel without fully parsing them
        """
        chunks = self._create_balanced_chunks(num_processes)
        
        manager = Manager()
        progress_dict = manager.dict()
        error_dict = manager.dict()
        
        with Pool(processes=num_processes) as pool:
            results = pool.starmap(
                self._count_chunk_safe,
                [(i, chunk[0], chunk[1], progress_dict, error_dict) for i, chunk in enumerate(chunks)]
            )
        

        if error_dict:
            print("\nErrors occurred during processing:")
            for worker_id, error in error_dict.items():
                print(f"  Worker {worker_id}: {error}")
            print("Continuing with available results...")
        

        total_records = sum(result for result in results if result is not None)
        return total_records
    
    def _parse_records_generator(self, num_processes=None):
        """
        Parse records and return a generator
        
        This is used for query2-4 which need the actual record data
        """
        record_count = 0
        start_time = time.time()
        last_time = start_time
        progress_interval = 5000000 
        
        try:
            with open(self.filepath, 'r', encoding='utf-8') as file:
                for line in file:
                    line = line.strip()
                    if line:
                        record = self._parse_line_fast(line)
                        record_count += 1
                        
                        if record_count % progress_interval == 0:
                            pass
                        
                        yield record
                        
            elapsed = time.time() - start_time
            print(f"Parsing completed: {record_count:,} records in {elapsed:.2f} seconds")
            print(f"Average speed: {record_count/elapsed:.2f} records/second")
            
        except Exception as e:
            print(f"Error parsing records: {str(e)}")
    
    def _create_balanced_chunks(self, num_processes):
        """
        Create balanced chunks that respect record boundaries
        """
        file_size = self.file_size
        approx_chunk_size = file_size // num_processes
        chunks = []
        
        with open(self.filepath, 'rb') as f:
            for i in range(num_processes):
                start_pos = i * approx_chunk_size
                
                if i > 0:  # No need to adjust start of first chunk
                    f.seek(start_pos)
                    # Read to next line boundary
                    f.readline()
                    start_pos = f.tell()
                
                # Determine end position (next chunk start or EOF)
                if i < num_processes - 1:
                    next_start = (i + 1) * approx_chunk_size
                    f.seek(next_start)
                    f.readline()  
                    end_pos = f.tell()
                else:
                    end_pos = file_size
                
                chunks.append((start_pos, end_pos))
        
        return chunks
    
    def _count_chunk_safe(self, worker_id, start_pos, end_pos, progress_dict, error_dict):
        """
        Process a chunk with proper error handling for counting only
        """
        try:
            return self._count_chunk(worker_id, start_pos, end_pos, progress_dict)
        except Exception as e:
            error_message = f"Error: {str(e)}\n{traceback.format_exc()}"
            error_dict[worker_id] = error_message
            print(f"Worker {worker_id} failed: {str(e)}")
            return 0  
    
    def _count_chunk(self, worker_id, start_pos, end_pos, progress_dict):
        """
        Process a chunk of the file but only count records (optimized for query1)
        """
        count = 0
        
        try:
            with open(self.filepath, 'r', encoding='utf-8') as file:
                file.seek(start_pos)
                
                current_pos = file.tell()
                
                while current_pos < end_pos:
                    line = file.readline()
                    if not line:  
                        break
                    
                    line = line.strip()
                    if line:
                        count += 1
                    
                    current_pos = file.tell()
            
            progress_dict[worker_id] = count
            return count
            
        except Exception as e:
            print(f"Error in worker {worker_id}: {str(e)}")
            raise 
    
    def _parse_line_fast(self, line):
        """Fast line parsing with built-in functions"""
        data = {}
        
        pos = line.find('{')
        if pos == -1:
            return data
        
        pos += 1 
        
        for idx, (field_name, field_type) in enumerate(self.expected_fields):
            field_marker = self.field_markers[idx]
            field_pos = line.find(field_marker, pos)
            
            if field_pos == -1:  
                data[field_name] = None
                continue
            
            value_start = line.find(':', field_pos + len(field_marker)) + 1
            while value_start < len(line) and line[value_start] == ' ':
                value_start += 1
            
            if value_start >= len(line):
                data[field_name] = None
                continue
            
            if field_type == str:
                if line[value_start] == '"':

                    string_start = value_start + 1
                    string_end = line.find('"', string_start)
                    if string_end == -1:
                        data[field_name] = None
                    else:
                        data[field_name] = line[string_start:string_end]
                        pos = string_end + 1
                else:
                    data[field_name] = None
            elif field_type in (int, float):
                value_end = line.find(',', value_start)
                if value_end == -1:
                    value_end = line.find('}', value_start)
                
                if value_end == -1:
                    data[field_name] = None
                else:
                    value_str = line[value_start:value_end].strip()
                    try:
                        data[field_name] = int(value_str) if field_type == int else float(value_str)
                    except ValueError:
                        data[field_name] = None
                    pos = value_end
        
        return data

def parse_json_file(filepath, query_type=None, num_processes=None):
    """
    Parse JSON file based on query type
    
    Args:
        filepath: Path to the JSON file
        query_type: The type of query being executed (for optimizations)
        num_processes: Number of processes to use (defaults to CPU count)
    
    Returns:
        For query1: Returns the total count directly
        For other queries: Returns a generator of records
    """
    parser = CustomJsonParser(filepath)
    return parser.parse_file(query_type, num_processes)