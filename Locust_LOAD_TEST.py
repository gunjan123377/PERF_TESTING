from locust import HttpUser, task, between, events
import pandas as pd
import os
import threading

class CSVDataUser(HttpUser):
    # Configure for 25 TPKS (Transactions Per Thousand Seconds)
    wait_time = between(1, 2)  # Faster execution with multiple users
    
    # Class-level variables for shared data
    csv_data = []
    data_index = 0
    data_lock = threading.Lock()
    csv_exhausted = False
    
    def on_start(self):
        """Load CSV data when first user starts"""
        if not self.csv_data:
            self.load_csv_data()
    
    @classmethod
    def load_csv_data(cls):
        """Load data from CSV file using pandas"""
        csv_file = 'test_data.csv'  # Change to your CSV file path
        
        if not os.path.exists(csv_file):
            print(f"Creating sample {csv_file} file...")
            cls.create_sample_csv(csv_file)
        
        try:
            # Read CSV using pandas
            df = pd.read_csv(csv_file)
            cls.csv_data = df['user_id'].tolist()  # Convert single column to list
            print(f"Loaded {len(cls.csv_data)} records from {csv_file}")
            print(f"Sample data: {cls.csv_data[:5]}")  # Show first 5 records
        except Exception as e:
            print(f"Error loading CSV: {e}")
            cls.csv_data = []
    
    @classmethod
    def create_sample_csv(cls, filename):
        """Create sample CSV file with single column"""
        import pandas as pd
        
        # Create sample data with single column
        sample_data = {
            'user_id': ['USER001', 'USER002', 'USER003', 'USER004', 'USER005', 
                       'USER006', 'USER007', 'USER008', 'USER009', 'USER010']
        }
        
        df = pd.DataFrame(sample_data)
        df.to_csv(filename, index=False)
        print(f"Created sample CSV file: {filename}")
    
    def get_next_data_item(self):
        """Get next item from CSV data thread-safely"""
        with self.data_lock:
            if self.data_index >= len(self.csv_data):
                self.csv_exhausted = True
                return None
            
            data_item = self.csv_data[self.data_index]
            self.data_index += 1
            return data_item
    
    @task
    def api_sequence_test(self):
        """Execute API sequence with CSV data"""
        
        # Check if CSV data is exhausted
        if self.csv_exhausted:
            print("CSV data exhausted. Stopping user.")
            self.environment.runner.quit()
            return
        
        # Get next data item from CSV
        user_id = self.get_next_data_item()
        if user_id is None:
            return
        
        print(f"Processing record {self.data_index}/{len(self.csv_data)}: {user_id}")
        
        # API 1: Create User
        create_payload = {
            "user_id": user_id,
            "name": f"User {user_id}",
            "email": f"{user_id.lower()}@example.com"
        }
        
        with self.client.post("/api/users", 
                            json=create_payload,
                            name="01_Create_User",
                            catch_response=True) as response:
            if response.status_code in [200, 201]:
                response.success()
            else:
                response.failure(f"Create User failed: {response.status_code}")
                return  # Stop sequence if create fails
        
        # API 2: Get User Details
        with self.client.get(f"/api/users/{user_id}",
                           name="02_Get_User",
                           catch_response=True) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Get User failed: {response.status_code}")
        
        # API 3: Create Transaction
        transaction_payload = {
            "user_id": user_id,
            "amount": 100.00,  # Fixed amount since CSV has only user_id
            "description": f"Transaction for {user_id}"
        }
        
        with self.client.post("/api/transactions",
                            json=transaction_payload,
                            name="03_Create_Transaction",
                            catch_response=True) as response:
            if response.status_code in [200, 201]:
                response.success()
            else:
                response.failure(f"Create Transaction failed: {response.status_code}")
                return
        
        # API 4: Update User
        update_payload = {
            "last_transaction_amount": 100.00,
            "status": "active"
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

# Event handlers for test lifecycle
@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    """Initialize test"""
    print("=== Starting CSV-Driven Load Test ===")
    print(f"Target Load: 25 TPKS (25 transactions per 1000 seconds)")
    print(f"CSV Records: {len(CSVDataUser.csv_data)}")

@events.test_stop.add_listener
def on_test_stop(environment, **kwargs):
    """Test completion handler"""
    print("=== Load Test Completed ===")
    print(f"Processed {CSVDataUser.data_index} out of {len(CSVDataUser.csv_data)} records")
    
    # Print summary
    stats = environment.runner.stats
    print(f"Total Requests: {stats.total.num_requests}")
    print(f"Total Failures: {stats.total.num_failures}")
    print(f"Average Response Time: {stats.total.avg_response_time:.2f}ms")

# Custom load shape for 25 TPKS
from locust import LoadTestShape

class TPKSLoadShape(LoadTestShape):
    """
    Custom load shape for 25 TPKS (Transactions Per Thousand Seconds)
    25 TPKS = 0.025 TPS
    With 5 APIs per sequence = 0.005 sequences per second
    Using 10 users with 2-second intervals = ~0.005 sequences/sec = 25 TPKS
    """
    
    def tick(self):
        run_time = self.get_run_time()
        
        # Stop when CSV data is exhausted
        if CSVDataUser.csv_exhausted:
            return None
        
        # Maintain steady 10 users for 25 TPKS target
        if run_time < 3600:  # Run for max 1 hour or until CSV exhausted
            return (10, 2)  # 10 users, spawn rate 2 per second
        
        return None