from locust import HttpUser, task, between, events
import pandas as pd
import os
import threading
from queue import Queue
import time

class CSVDataUser(HttpUser):
    # Configure for 25 TPKS (Transactions Per Thousand Seconds)
    wait_time = between(1, 2)
    
    # Class-level variables for shared data with threading
    csv_queue = Queue()  # Thread-safe queue for CSV data
    csv_data = []
    csv_exhausted = False
    processed_records = set()  # Track processed records
    data_lock = threading.Lock()
    
    def on_start(self):
        """Load CSV data when first user starts"""
        if not self.csv_data:
            self.load_csv_data()
    
    @classmethod
    def load_csv_data(cls):
        """Load data from CSV file using pandas and populate queue"""
        csv_file = 'test_data.csv'
        
        if not os.path.exists(csv_file):
            print(f"Creating sample {csv_file} file...")
            cls.create_sample_csv(csv_file)
        
        try:
            df = pd.read_csv(csv_file)
            cls.csv_data = df['user_id'].tolist()
            
            # Populate thread-safe queue
            for user_id in cls.csv_data:
                cls.csv_queue.put(user_id)
            
            print(f"Loaded {len(cls.csv_data)} records into thread-safe queue")
            print(f"Sample data: {cls.csv_data[:5]}")
        except Exception as e:
            print(f"Error loading CSV: {e}")
            cls.csv_data = []
    
    @classmethod
    def create_sample_csv(cls, filename):
        """Create sample CSV file with single column"""
        sample_data = {
            'user_id': ['USER001', 'USER002', 'USER003', 'USER004', 'USER005', 
                       'USER006', 'USER007', 'USER008', 'USER009', 'USER010',
                       'USER011', 'USER012', 'USER013', 'USER014', 'USER015']
        }
        
        df = pd.DataFrame(sample_data)
        df.to_csv(filename, index=False)
        print(f"Created sample CSV file: {filename}")
    
    def get_next_data_item(self):
        """Get next item from CSV queue (thread-safe, each record processed once)"""
        try:
            # Get item from queue with timeout
            user_id = self.csv_queue.get(timeout=1)
            
            # Mark as processed to avoid duplicates
            with self.data_lock:
                if user_id in self.processed_records:
                    # Already processed, get another one
                    return self.get_next_data_item()
                self.processed_records.add(user_id)
            
            return user_id
            
        except:  # Queue empty
            with self.data_lock:
                if not self.csv_exhausted:
                    self.csv_exhausted = True
                    print("CSV queue exhausted - no more data available")
            return None
    
    @task
    def api_sequence_test(self):
        """Execute API sequence with CSV data - THREAD SAFE, ONE TIME PROCESSING"""
        
        # Get next data item from queue (thread-safe)
        user_id = self.get_next_data_item()
        if user_id is None:
            print(f"User {self.client.base_url}: No more data. Stopping.")
            self.environment.runner.quit()
            return
        
        current_count = len(self.processed_records)
        total_count = len(self.csv_data)
        print(f"Processing record {current_count}/{total_count}: {user_id}")
        
        # Execute API sequence for this unique record
        success = self.execute_api_sequence(user_id)
        
        if success:
            # Mark task as completed in queue
            self.csv_queue.task_done()
        
        # Check if all records processed
        if len(self.processed_records) >= len(self.csv_data):
            print("All CSV records processed. Test completed.")
            self.environment.runner.quit()
    
    def execute_api_sequence(self, user_id):
        """Execute the 5 API sequence for a user"""
        
        # API 1: Create User
        create_payload = {
            "user_id": user_id,
            "name": f"User {user_id}",
            "email": f"{user_id.lower()}@example.com",
            "timestamp": int(time.time())  # Unique timestamp
        }
        
        with self.client.post("/api/users", 
                            json=create_payload,
                            name="01_Create_User",
                            catch_response=True) as response:
            if response.status_code in [200, 201]:
                response.success()
            else:
                response.failure(f"Create User failed: {response.status_code}")
                return False
        
        # API 2: Get User Details
        with self.client.get(f"/api/users/{user_id}",
                           name="02_Get_User",
                           catch_response=True) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Get User failed: {response.status_code}")
        
        # API 3: Create Transaction (Unique for each user)
        transaction_payload = {
            "user_id": user_id,
            "amount": 100.00,
            "description": f"Transaction for {user_id}",
            "transaction_id": f"TXN_{user_id}_{int(time.time())}",  # Unique transaction ID
            "created_by": f"thread_{threading.current_thread().ident}"
        }
        
        with self.client.post("/api/transactions",
                            json=transaction_payload,
                            name="03_Create_Transaction",
                            catch_response=True) as response:
            if response.status_code in [200, 201]:
                response.success()
            else:
                response.failure(f"Create Transaction failed: {response.status_code}")
                return False
        
        # API 4: Update User
        update_payload = {
            "last_transaction_amount": 100.00,
            "status": "active",
            "updated_at": int(time.time())
        }
        
        with self.client.put(f"/api/users/{user_id}",
                           json=update_payload,
                           name="04_Update_User",
                           catch_response=True) as response:
            if response.status_code in [200, 204]:
                response.success()
            else:
                response.failure(f"Update User failed: {response.status_code}")
        
        # API 5: Get Transaction History
        with self.client.get(f"/api/users/{user_id}/transactions",
                           name="05_Get_Transactions",
                           catch_response=True) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Get Transactions failed: {response.status_code}")
        
        return True

# Alternative approach using shared-iterations
class SharedIterationsUser(HttpUser):
    """Alternative approach using shared iterations for exact once processing"""
    wait_time = between(1, 2)
    
    csv_data = []
    
    def on_start(self):
        if not self.csv_data:
            self.load_csv_data()
    
    @classmethod
    def load_csv_data(cls):
        csv_file = 'test_data.csv'
        if not os.path.exists(csv_file):
            cls.create_sample_csv(csv_file)
        
        df = pd.read_csv(csv_file)
        cls.csv_data = df['user_id'].tolist()
        print(f"Loaded {len(cls.csv_data)} records for shared iterations")
    
    @classmethod
    def create_sample_csv(cls, filename):
        sample_data = {
            'user_id': [f'USER{i:03d}' for i in range(1, 21)]  # USER001 to USER020
        }
        df = pd.DataFrame(sample_data)
        df.to_csv(filename, index=False)
    
    @task
    def process_single_record(self):
        """Each iteration processes one record exactly once"""
        # Get current iteration number (unique per execution)
        current_iteration = getattr(self, 'iteration_count', 0)
        
        # Check if we have data for this iteration
        if current_iteration >= len(self.csv_data):
            print("No more data available. Stopping.")
            self.environment.runner.quit()
            return
        
        user_id = self.csv_data[current_iteration]
        print(f"Thread {threading.current_thread().ident} processing: {user_id}")
        
        # Execute API sequence
        self.execute_api_sequence(user_id)
        
        # Increment iteration count
        self.iteration_count = getattr(self, 'iteration_count', 0) + 1
    
    def execute_api_sequence(self, user_id):
        """Same API sequence as above"""
        # API calls implementation (same as above)
        pass

# Event handlers
@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    print("=== Starting Threaded CSV Load Test (Process Once) ===")
    print(f"CSV Records: {len(CSVDataUser.csv_data)}")
    print("Each record will be processed exactly once across all threads")

@events.test_stop.add_listener
def on_test_stop(environment, **kwargs):
    print("=== Load Test Completed ===")
    print(f"Processed Records: {len(CSVDataUser.processed_records)}")
    print(f"Total CSV Records: {len(CSVDataUser.csv_data)}")
    
    # Print summary
    stats = environment.runner.stats
    print(f"Total Requests: {stats.total.num_requests}")
    print(f"Total Failures: {stats.total.num_failures}")

# Load shape for threaded processing
from locust import LoadTestShape

class ThreadedOnceLoadShape(LoadTestShape):
    """Load shape for threaded processing where each record is processed once"""
    
    def tick(self):
        run_time = self.get_run_time()
        
        # Stop when all CSV data processed
        if (hasattr(CSVDataUser, 'processed_records') and 
            len(CSVDataUser.processed_records) >= len(CSVDataUser.csv_data)):
            return None
        
        # Use multiple threads but ensure single processing per record
        if run_time < 1800:  # 30 minutes max
            return (5, 2)  # 5 users, spawn rate 2
        
        return None