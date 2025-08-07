import asyncio
import aiohttp
import pandas as pd
import time

class VehicleGenerator:
    def __init__(self, api_url: str, second_api_url: str, auth_token: str = None):
        self.api_url = api_url
        self.second_api_url = second_api_url
        self.headers = {'Content-Type': 'application/json'}
        if auth_token:
            self.headers['Authorization'] = f'Bearer {auth_token}'
        
        self.vehicle_ids = []
    
    def create_payload(self, index: int) -> dict:
        """Create payload for first API - MODIFY THIS"""
        return {
            "vehicle_number": f"VH{index:06d}",
            "model": f"Model_{index % 10}",
            "year": 2020 + (index % 5),
            "owner_id": f"OWNER_{index}"
        }
    
    def create_second_payload(self, vehicle_id: str, index: int) -> dict:
        """Create payload for second API - MODIFY THIS"""
        return {
            "status": "active",
            "updated_by": f"SYSTEM_{index}"
        }
    
    async def create_vehicle_sequence(self, session: aiohttp.ClientSession, index: int):
        """Run both APIs in sequence for one vehicle"""
        # First API - Create vehicle
        payload1 = self.create_payload(index)
        async with session.post(self.api_url, json=payload1, headers=self.headers) as response:
            data = await response.json()
            vehicle_id = data.get('vehicle_id') or data.get('id')
        
        # Second API - Update vehicle using vehicle_id
        payload2 = self.create_second_payload(vehicle_id, index)
        second_url = f"{self.second_api_url}/{vehicle_id}"
        async with session.put(second_url, json=payload2, headers=self.headers) as response:
            pass  # Just run the API, no validation
        
        return vehicle_id
    
    async def generate_vehicles(self, total: int = 100000, concurrent: int = 50):
        """Generate vehicles with both APIs running in sequence"""
        print(f"Creating {total} vehicles (2 APIs each)...")
        start_time = time.time()
        
        async with aiohttp.ClientSession() as session:
            tasks = [self.create_vehicle_sequence(session, i) for i in range(total)]
            
            for i, task in enumerate(asyncio.as_completed(tasks)):
                vehicle_id = await task
                self.vehicle_ids.append(vehicle_id)
                
                if (i + 1) % 5000 == 0:
                    elapsed = time.time() - start_time
                    rate = (i + 1) / elapsed
                    print(f"Completed: {i+1}/{total} | Rate: {rate:.1f}/sec")
        
        total_time = time.time() - start_time
        print(f"\nCompleted {len(self.vehicle_ids)} vehicles in {total_time:.1f} seconds")
        return self.vehicle_ids
    
    def save_to_csv(self, filename: str = "vehicle_ids.csv"):
        """Save vehicle IDs to CSV"""
        df = pd.DataFrame({'vehicle_id': self.vehicle_ids})
        df.to_csv(filename, index=False)
        print(f"Saved {len(self.vehicle_ids)} vehicle IDs to {filename}")

# USAGE
async def main():
    # CONFIGURE YOUR APIs
    API_URL = "https://your-api.com/vehicles"           # First API (POST)
    SECOND_API_URL = "https://your-api.com/vehicles"    # Second API (PUT /{vehicle_id})
    AUTH_TOKEN = "your-token"                           # Optional
    
    generator = VehicleGenerator(API_URL, SECOND_API_URL, AUTH_TOKEN)
    
    # Generate vehicles
    await generator.generate_vehicles(total=100000, concurrent=50)
    
    # Save to CSV
    generator.save_to_csv("vehicle_ids.csv")

if __name__ == "__main__":
    asyncio.run(main())
