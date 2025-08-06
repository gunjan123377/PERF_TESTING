# Install required packages
pip install locust pandas

# Run the load test with Web UI (for monitoring)
locust -f locustfile.py --host=http://localhost:8000

# Run headless with automatic stop when CSV is exhausted
locust -f locustfile.py --headless --users=10 --spawn-rate=2 --host=http://localhost:8000 --html=report.html

# Run with custom load shape for exact 25 TPKS
locust -f locustfile.py --headless --host=http://localhost:8000 --html=report.html

# Run with time limit as backup (in case CSV doesn't exhaust)
locust -f locustfile.py --headless --users=10 --spawn-rate=2 --run-time=30m --host=http://localhost:8000

# Generate detailed CSV reports
locust -f locustfile.py --headless --users=10 --spawn-rate=2 --host=http://localhost:8000 --csv=results --html=report.html

=========================================================================================================================
# 1. Install K6
brew install k6  # macOS
# or check setup commands for other platforms

# 2. Create CSV file (or use provided sample)

# 3. Run the test
k6 run k6_csv_external.js

# 4. With custom host
k6 run -e API_HOST=http://your-api-host k6_csv_external.js
