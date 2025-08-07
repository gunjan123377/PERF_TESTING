import asyncio
import aiohttp
import json
import time
import pandas as pd
from typing import List

class VehicleGenerator:
    def __init__(self, api_url: str, auth_token: str = None):
        self.api_url = api_url
        self.headers = {'Content-Type': 'application/json'}
        if auth_token:
            self.headers['Authorization'] = f'Bearer {auth_token}'
        
        self.vehicle_ids = []
        self.failed_count = 0
    
    def create_payload(self, index: int) -> dict:
        """Create unique payload for each vehicle - MODIFY THIS FOR YOUR API"""
        return {
            "vehicle_number": f"VH{index:06d}",
            "model": f"Model_{index % 10}",
            "year": 2020 + (index % 5),
            "owner_id": f"OWNER_{index}",
            "unique_key": f"{int(time.time() * 1000)}_{index}"
        }
    
    async def create_single_vehicle(self, session: aiohttp.ClientSession, index: int):
        """Create one vehicle and return its ID"""
        payload = self.create_payload(index)
        
        try:
            async with session.post(self.api_url, json=payload, headers=self.headers) as response:
                if response.status in [200, 201]:
                    data = await response.json()
                    # MODIFY: Extract vehicle ID based on your API response
                    vehicle_id = data.get('vehicle_id') or data.get('id')
                    return vehicle_id
                else:
                    self.failed_count += 1
                    return None
        except Exception:
            self.failed_count += 1
            return None
    
    async def generate_vehicles(self, total: int = 100000, concurrent: int = 50):
        """Main function to generate all vehicles"""
        print(f"Creating {total} vehicles with {concurrent} concurrent requests...")
        start_time = time.time()
        
        # Limit concurrent connections
        connector = aiohttp.TCPConnector(limit=concurrent)
        timeout = aiohttp.ClientTimeout(total=30)
        
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            # Create all tasks
            tasks = [self.create_single_vehicle(session, i) for i in range(total)]
            
            # Execute with progress tracking
            for i, task in enumerate(asyncio.as_completed(tasks)):
                vehicle_id = await task
                if vehicle_id:
                    self.vehicle_ids.append(vehicle_id)
                
                # Show progress every 5000 requests
                if (i + 1) % 5000 == 0:
                    elapsed = time.time() - start_time
                    rate = (i + 1) / elapsed
                    print(f"Completed: {i+1}/{total} | Rate: {rate:.1f}/sec")
        
        # Final summary
        total_time = time.time() - start_time
        success_count = len(self.vehicle_ids)
        
        print(f"\n=== RESULTS ===")
        print(f"Time taken: {total_time:.1f} seconds")
        print(f"Successful: {success_count}")
        print(f"Failed: {self.failed_count}")
        print(f"Success rate: {success_count/total*100:.1f}%")
        print(f"Average rate: {success_count/total_time:.1f} vehicles/sec")
        
        return self.vehicle_ids
    
    def save_to_file(self, filename: str = "vehicle_ids.csv", format: str = "csv"):
        """Save vehicle IDs to CSV or JSON file using pandas"""
        if format.lower() == "csv":
            # Create DataFrame
            df = pd.DataFrame({
                'vehicle_id': self.vehicle_ids,
                'index': range(len(self.vehicle_ids)),
                'created_timestamp': pd.Timestamp.now()
            })
            
            # Save to CSV
            df.to_csv(filename, index=False)
            print(f"Saved {len(self.vehicle_ids)} vehicle IDs to {filename}")
            
        elif format.lower() == "json":
            # Save as JSON using pandas
            df = pd.DataFrame({
                'vehicle_id': self.vehicle_ids,
                'total_count': len(self.vehicle_ids)
            })
            df.to_json(filename, orient='records', indent=2)
            print(f"Saved {len(self.vehicle_ids)} vehicle IDs to {filename}")
        
        else:
            print("Supported formats: 'csv' or 'json'")
    
    def save_detailed_csv(self, filename: str = "vehicle_details.csv"):
        """Save detailed vehicle information with metadata to CSV"""
        if not self.vehicle_ids:
            print("No vehicle IDs to save")
            return
        
        # Create detailed DataFrame with additional metadata
        vehicle_data = []
        for i, vehicle_id in enumerate(self.vehicle_ids):
            vehicle_data.append({
                'vehicle_id': vehicle_id,
                'sequence_number': i + 1,
                'vehicle_number': f"VH{i:06d}",
                'model': f"Model_{i % 10}",
                'year': 2020 + (i % 5),
                'owner_id': f"OWNER_{i}",
                'created_timestamp': pd.Timestamp.now(),
                'batch_id': f"BATCH_{int(time.time())}"
            })
        
        df = pd.DataFrame(vehicle_data)
        df.to_csv(filename, index=False)
        print(f"Saved detailed vehicle data to {filename}")
        
        # Display sample data
        print(f"\nSample data (first 5 rows):")
        print(df.head().to_string(index=False))

# USAGE EXAMPLE
async def main():
    # CONFIGURE THESE FOR YOUR API
    API_URL = "https://your-api-endpoint.com/vehicles"
    AUTH_TOKEN = "your-auth-token-here"  # Optional
    
    generator = VehicleGenerator(API_URL, AUTH_TOKEN)
    
    # Generate 100k vehicles (adjust numbers as needed)
    vehicle_ids = await generator.generate_vehicles(
        total=100000,      # Total vehicles to create
        concurrent=50      # Concurrent requests (tune based on API limits)
    )
    
    # Save results (multiple formats available)
    generator.save_to_file("vehicle_ids.csv", format="csv")          # Save as CSV
    generator.save_detailed_csv("vehicle_details.csv")              # Save with metadata
    # generator.save_to_file("vehicle_ids.json", format="json")     # Optional: JSON format
    
    return vehicle_ids

# RUN THE SCRIPT
if __name__ == "__main__":
    vehicle_ids = asyncio.run(main())
    print(f"Generated {len(vehicle_ids)} vehicle IDs successfully!")