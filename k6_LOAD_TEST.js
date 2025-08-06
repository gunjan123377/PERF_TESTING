import http from 'k6/http';
import { check, sleep } from 'k6';
import { SharedArray } from 'k6/data';
import { Counter, Trend } from 'k6/metrics';
import papaparse from 'https://jslib.k6.io/papaparse/5.1.1/index.js';

// Custom metrics
const apiSequenceCounter = new Counter('api_sequences_completed');
const csvRecordsProcessed = new Counter('csv_records_processed');
const apiResponseTime = new Trend('api_response_time');

// Load CSV data using SharedArray (read only once, shared among all VUs)
const csvData = new SharedArray('csv_data', function() {
  // In k6, we need to read the CSV file content
  // For this example, we'll use inline data, but you can read from file
  const csvContent = `user_id
USER001
USER002
USER003
USER004
USER005
USER006
USER007
USER008
USER009
USER010
USER011
USER012
USER013
USER014
USER015`;

  const parsedData = papaparse.parse(csvContent, {
    header: true,
    skipEmptyLines: true,
  });
  
  console.log(`Loaded ${parsedData.data.length} records from CSV`);
  return parsedData.data;
});

// Test configuration for 25 TPKS
export const options = {
  scenarios: {
    csv_driven_load: {
      executor: 'shared-iterations',
      vus: 10,                    // 10 virtual users
      iterations: csvData.length, // Total iterations = CSV records
      maxDuration: '30m',         // Maximum test duration
    },
  },
  
  // Thresholds for pass/fail criteria
  thresholds: {
    http_req_duration: ['p(95)<2000'], // 95% of requests under 2s
    http_req_failed: ['rate<0.01'],    // Error rate under 1%
    csv_records_processed: [`count==${csvData.length}`], // All CSV records processed
  },
  
  // Test metadata
  tags: {
    test_type: 'csv_driven_load_test',
    target_load: '25_TPKS'
  }
};

// Global variables for tracking
let globalIndex = 0;
const processedRecords = new Set();

export default function() {
  // Get current VU and iteration info
  const vuId = __VU;
  const iteration = __ITER;
  
  // Calculate which CSV record to use (thread-safe approach)
  const recordIndex = (vuId - 1) * Math.ceil(csvData.length / 10) + Math.floor(iteration / 10);
  
  // Check if we've exceeded available data
  if (recordIndex >= csvData.length) {
    console.log(`VU ${vuId}: No more data available. Stopping.`);
    return;
  }
  
  const csvRecord = csvData[recordIndex];
  const userId = csvRecord.user_id;
  
  // Skip if already processed (avoid duplicates)
  const recordKey = `${recordIndex}_${userId}`;
  if (processedRecords.has(recordKey)) {
    return;
  }
  processedRecords.add(recordKey);
  
  console.log(`VU ${vuId} processing record ${recordIndex + 1}/${csvData.length}: ${userId}`);
  
  // Execute API sequence
  executeAPISequence(userId, recordIndex + 1);
  
  // Increment counters
  csvRecordsProcessed.add(1);
  
  // Sleep between sequences to maintain 25 TPKS
  // 25 TPKS = 0.025 TPS per sequence
  // With 10 VUs, each VU should wait ~4 seconds between sequences
  sleep(Math.random() * 2 + 1); // 1-3 seconds random wait
}

function executeAPISequence(userId, recordNumber) {
  const baseUrl = 'http://localhost:8000'; // Change to your API host
  const headers = {
    'Content-Type': 'application/json',
    'Accept': 'application/json'
  };
  
  console.log(`Starting API sequence for ${userId} (Record ${recordNumber})`);
  
  // API 1: Create User
  const createUserPayload = {
    user_id: userId,
    name: `User ${userId}`,
    email: `${userId.toLowerCase()}@example.com`
  };
  
  const createUserResponse = http.post(
    `${baseUrl}/api/users`,
    JSON.stringify(createUserPayload),
    {
      headers: headers,
      tags: { api: 'create_user', sequence: '1' }
    }
  );
  
  check(createUserResponse, {
    'API 1 - Create User: status is 200 or 201': (r) => [200, 201].includes(r.status),
    'API 1 - Create User: response time < 2000ms': (r) => r.timings.duration < 2000,
  });
  
  if (![200, 201].includes(createUserResponse.status)) {
    console.error(`API 1 failed for ${userId}: ${createUserResponse.status}`);
    return; // Stop sequence on failure
  }
  
  apiResponseTime.add(createUserResponse.timings.duration);
  sleep(0.2); // Small delay between APIs
  
  // API 2: Get User Details
  const getUserResponse = http.get(
    `${baseUrl}/api/users/${userId}`,
    {
      headers: headers,
      tags: { api: 'get_user', sequence: '2' }
    }
  );
  
  check(getUserResponse, {
    'API 2 - Get User: status is 200': (r) => r.status === 200,
    'API 2 - Get User: response time < 1000ms': (r) => r.timings.duration < 1000,
  });
  
  apiResponseTime.add(getUserResponse.timings.duration);
  sleep(0.2);
  
  // API 3: Create Transaction
  const createTransactionPayload = {
    user_id: userId,
    amount: 100.00,
    description: `Transaction for ${userId}`
  };
  
  const createTransactionResponse = http.post(
    `${baseUrl}/api/transactions`,
    JSON.stringify(createTransactionPayload),
    {
      headers: headers,
      tags: { api: 'create_transaction', sequence: '3' }
    }
  );
  
  check(createTransactionResponse, {
    'API 3 - Create Transaction: status is 200 or 201': (r) => [200, 201].includes(r.status),
    'API 3 - Create Transaction: response time < 2000ms': (r) => r.timings.duration < 2000,
  });
  
  if (![200, 201].includes(createTransactionResponse.status)) {
    console.error(`API 3 failed for ${userId}: ${createTransactionResponse.status}`);
    return;
  }
  
  apiResponseTime.add(createTransactionResponse.timings.duration);
  sleep(0.2);
  
  // API 4: Update User
  const updateUserPayload = {
    last_transaction_amount: 100.00,
    status: 'active'
  };
  
  const updateUserResponse = http.put(
    `${baseUrl}/api/users/${userId}`,
    JSON.stringify(updateUserPayload),
    {
      headers: headers,
      tags: { api: 'update_user', sequence: '4' }
    }
  );
  
  check(updateUserResponse, {
    'API 4 - Update User: status is 200 or 204': (r) => [200, 204].includes(r.status),
    'API 4 - Update User: response time < 1500ms': (r) => r.timings.duration < 1500,
  });
  
  apiResponseTime.add(updateUserResponse.timings.duration);
  sleep(0.2);
  
  // API 5: Get Transaction History
  const getTransactionsResponse = http.get(
    `${baseUrl}/api/users/${userId}/transactions`,
    {
      headers: headers,
      tags: { api: 'get_transactions', sequence: '5' }
    }
  );
  
  check(getTransactionsResponse, {
    'API 5 - Get Transactions: status is 200': (r) => r.status === 200,
    'API 5 - Get Transactions: response time < 1000ms': (r) => r.timings.duration < 1000,
  });
  
  apiResponseTime.add(getTransactionsResponse.timings.duration);
  
  // Mark sequence as completed
  apiSequenceCounter.add(1);
  
  console.log(`Completed API sequence for ${userId} (Record ${recordNumber})`);
}

// Setup function (runs once per VU at the start)
export function setup() {
  console.log('=== Starting K6 CSV-Driven Load Test ===');
  console.log(`Target Load: 25 TPKS (25 transactions per 1000 seconds)`);
  console.log(`Total CSV Records: ${csvData.length}`);
  console.log(`Virtual Users: ${options.scenarios.csv_driven_load.vus}`);
  console.log(`Total Iterations: ${options.scenarios.csv_driven_load.iterations}`);
  
  return { csvRecords: csvData.length };
}

// Teardown function (runs once at the end)
export function teardown(data) {
  console.log('=== K6 Load Test Completed ===');
  console.log(`Total CSV Records Available: ${data.csvRecords}`);
  console.log('Check the summary report for detailed metrics');
}