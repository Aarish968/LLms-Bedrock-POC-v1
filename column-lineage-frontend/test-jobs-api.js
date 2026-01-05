// Simple test script to verify the jobs API endpoints work
const BASE_URL = 'http://localhost:8000';

async function testJobsAPI() {
  console.log('🧪 Testing Jobs API Endpoints');
  console.log('=' * 40);

  try {
    // Test list jobs endpoint
    console.log('\n1. Testing list jobs endpoint...');
    const response = await fetch(`${BASE_URL}/api/v1/lineage/jobs`, {
      headers: {
        'Authorization': 'Bearer YOUR_TOKEN_HERE', // Replace with actual token
        'Content-Type': 'application/json'
      }
    });

    console.log(`Status: ${response.status}`);
    
    if (response.ok) {
      const jobs = await response.json();
      console.log(`✅ Found ${jobs.length} jobs`);
      
      if (jobs.length > 0) {
        const firstJob = jobs[0];
        console.log(`First job: ${firstJob.job_id} - Status: ${firstJob.status}`);
        
        // Test job status endpoint
        console.log('\n2. Testing job status endpoint...');
        const statusResponse = await fetch(`${BASE_URL}/api/v1/lineage/status/${firstJob.job_id}`, {
          headers: {
            'Authorization': 'Bearer YOUR_TOKEN_HERE',
            'Content-Type': 'application/json'
          }
        });
        
        if (statusResponse.ok) {
          const status = await statusResponse.json();
          console.log(`✅ Job status: ${status.status}`);
          console.log(`   Progress: ${status.processed_views}/${status.total_views}`);
          
          // Test results endpoint if completed
          if (status.status === 'COMPLETED') {
            console.log('\n3. Testing results endpoint...');
            const resultsResponse = await fetch(`${BASE_URL}/api/v1/lineage/results/${firstJob.job_id}`, {
              headers: {
                'Authorization': 'Bearer YOUR_TOKEN_HERE',
                'Content-Type': 'application/json'
              }
            });
            
            if (resultsResponse.ok) {
              const results = await resultsResponse.json();
              console.log(`✅ Results: ${results.total_results} lineage relationships`);
            } else {
              console.log(`❌ Results failed: ${resultsResponse.status}`);
            }
          }
        } else {
          console.log(`❌ Status failed: ${statusResponse.status}`);
        }
      }
    } else {
      console.log(`❌ List jobs failed: ${response.status}`);
      const error = await response.text();
      console.log(`Error: ${error}`);
    }
    
  } catch (error) {
    console.log(`❌ Test failed: ${error.message}`);
  }
}

// Run the test
testJobsAPI();