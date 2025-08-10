/**
 * Google Ads Account Debug Module - Refactored for centralized architecture
 * Receives authenticated API client from index.js instead of creating its own
 * Focuses on debugging account access and MCC relationships
 */

/**
 * Debug test account access and MCC relationship
 * @param {Object} params - Parameters from index.js
 * @param {Object} params.apiClient - Google Ads API client from index.js
 * @param {Object} params.headers - Authenticated headers
 * @param {string} params.testAccountId - Test account ID
 * @param {string} params.mccId - MCC account ID
 * @returns {Object} Debug results
 */
async function debugTestAccountAccess({ apiClient, headers, testAccountId, mccId }) {
  try {
    console.log('🔍 Debugging TEST ACCOUNT access...');
    
    const results = {
      mcc_access: null,
      test_account_with_mcc: null,
      customer_hierarchy: null,
      api_client_test: null,
      final_recommendation: null
    };
    
    console.log('📋 Test Account Configuration:');
    console.log(`   Headers present: ${headers ? 'Yes' : 'No'}`);
    console.log(`   Test Account ID: ${testAccountId}`);
    console.log(`   MCC ID: ${mccId}`);
    console.log(`   MCC Header: ${headers['login-customer-id'] || 'Not set'}`);
    
    // Test 1: Direct MCC access
    console.log('\n1️⃣ Testing MCC direct access...');
    try {
      const response = await apiClient.getCustomer(mccId, {
        ...headers,
        'login-customer-id': undefined // Remove MCC header for direct access
      });
      
      results.mcc_access = { success: true };
      
      console.log('✅ MCC access successful!');
      if (response.data.results && response.data.results.length > 0) {
        const customer = response.data.results[0].customer;
        results.mcc_access.details = {
          id: customer.id,
          name: customer.descriptiveName,
          is_manager: customer.manager
        };
        console.log(`   MCC Account: ${customer.descriptiveName} (${customer.id})`);
        console.log(`   Is Manager: ${customer.manager}`);
      }
      
    } catch (error) {
      results.mcc_access = { 
        success: false, 
        error: error.response?.data?.error?.message || error.message,
        status: error.response?.status
      };
      console.log(`❌ MCC access failed: ${error.response?.status} - ${error.response?.data?.error?.message || error.message}`);
    }
    
    // Test 2: Test account with MCC login header
    console.log('\n2️⃣ Testing test account with MCC login header...');
    try {
      const response = await apiClient.getCustomer(testAccountId, headers);
      
      results.test_account_with_mcc = { success: true };
      
      console.log('✅ SUCCESS with MCC login customer header!');
      if (response.data.results && response.data.results.length > 0) {
        const customer = response.data.results[0].customer;
        results.test_account_with_mcc.details = {
          id: customer.id,
          name: customer.descriptiveName,
          is_test: customer.testAccount
        };
        console.log(`   Account: ${customer.descriptiveName} (${customer.id})`);
        console.log(`   Test Account: ${customer.testAccount}`);
      }
      
    } catch (error) {
      results.test_account_with_mcc = { 
        success: false, 
        error: error.response?.data?.error?.message || error.message,
        status: error.response?.status
      };
      console.log(`❌ Failed with MCC header: ${error.response?.status} - ${error.response?.data?.error?.message || error.message}`);
    }
    
    // Test 3: Customer hierarchy under MCC
    console.log('\n3️⃣ Listing customers under MCC...');
    try {
      const response = await apiClient.getCustomerHierarchy(mccId, {
        ...headers,
        'login-customer-id': undefined // Remove MCC header for direct MCC access
      });
      
      results.customer_hierarchy = { success: true, customers: [] };
      
      console.log('✅ Customer list retrieved from MCC!');
      if (response.data.results && response.data.results.length > 0) {
        console.log('\n   📋 Customers under MCC:');
        response.data.results.forEach((result, index) => {
          const client = result.customerClient;
          const testAccount = client.testAccount ? '(TEST)' : '(PROD)';
          const isTarget = client.clientCustomer === testAccountId ? ' ← TARGET' : '';
          
          results.customer_hierarchy.customers.push({
            id: client.clientCustomer,
            name: client.descriptiveName,
            is_test: client.testAccount,
            is_target: client.clientCustomer === testAccountId
          });
          
          console.log(`      ${index + 1}. ${client.descriptiveName} - ${client.clientCustomer} ${testAccount}${isTarget}`);
        });
      }
      
    } catch (error) {
      results.customer_hierarchy = { 
        success: false, 
        error: error.response?.data?.error?.message || error.message,
        status: error.response?.status
      };
      console.log(`❌ Customer list failed: ${error.response?.status} - ${error.response?.data?.error?.message || error.message}`);
    }
    
    // Test 4: API client functionality test
    console.log('\n4️⃣ Testing API client functionality...');
    results.api_client_test = { client_working: false };
    
    try {
      // Test if our centralized API client is working
      await apiClient.getCustomer(mccId, {
        ...headers,
        'login-customer-id': undefined
      });
      
      console.log(`   ✅ Centralized API client works`);
      results.api_client_test.client_working = true;
      
    } catch (error) {
      console.log(`   ❌ API client failed: ${error.response?.status}`);
    }
    
    // Generate final recommendation
    results.final_recommendation = generateDebugRecommendation(results, { testAccountId, mccId });
    
    return {
      success: true,
      message: 'Debug analysis completed',
      results: results,
      timestamp: new Date().toISOString()
    };
    
  } catch (error) {
    console.error('💥 Debug script failed:', error.message);
    return {
      success: false,
      error: error.message,
      timestamp: new Date().toISOString()
    };
  }
}


/**
 * Debug campaign targeting specifically
 * @param {Object} customer - Google Ads customer client
 * @param {string} campaignId - Campaign ID to debug
 * @returns {Object} Debug results
 */
async function debugCampaignTargeting(customer, campaignId) {
  try {
    console.log(`🔍 DEBUG: Testing targeting query for campaign: ${campaignId}`);
    
    // Step 1: Check ALL criteria types first
    const allCriteriaQuery = `
      SELECT 
        campaign_criterion.campaign,
        campaign_criterion.criterion_id,
        campaign_criterion.type,
        campaign_criterion.negative,
        campaign_criterion.status
      FROM campaign_criterion
      WHERE campaign.id = ${campaignId}
    `;
    
    console.log('📊 Executing ALL criteria query...');
    const allResults = await customer.query(allCriteriaQuery);
    console.log(`✅ Found ${allResults.length} total criteria of all types`);
    
    allResults.forEach((row, index) => {
      console.log(`   ${index + 1}. Type: ${row.campaign_criterion.type}, Negative: ${row.campaign_criterion.negative}, Status: ${row.campaign_criterion.status}`);
    });
    
    // Step 2: Specifically look for location criteria
    const locationCriteriaQuery = `
      SELECT 
        campaign_criterion.campaign,
        campaign_criterion.criterion_id,
        campaign_criterion.location.geo_target_constant,
        campaign_criterion.negative,
        campaign_criterion.status
      FROM campaign_criterion
      WHERE campaign.id = ${campaignId}
        AND campaign_criterion.type = 'LOCATION'
    `;
    
    console.log('📍 Executing location criteria query...');
    const locationResults = await customer.query(locationCriteriaQuery);
    console.log(`✅ Found ${locationResults.length} location criteria`);
    
    locationResults.forEach((row, index) => {
      console.log(`   ${index + 1}. Geo Target: ${row.campaign_criterion.location?.geo_target_constant}, Negative: ${row.campaign_criterion.negative}`);
    });
    
    return {
      success: true,
      campaign_id: campaignId,
      all_criteria: allResults.length,
      location_criteria: locationResults.length,
      all_criteria_details: allResults,
      location_criteria_details: locationResults,
      timestamp: new Date().toISOString()
    };
    
  } catch (error) {
    console.error('❌ DEBUG: Query failed:', error.message);
    return {
      success: false,
      error: error.message,
      timestamp: new Date().toISOString()
    };
  }
}


/**
 * Generate debug recommendation based on test results
 */
function generateDebugRecommendation(results, { testAccountId, mccId }) {
  const recommendation = {
    status: 'unknown',
    message: '',
    next_steps: [],
    optimal_config: null
  };
  
  // Check if test account access is working
  if (results.test_account_with_mcc?.success) {
    recommendation.status = 'working';
    recommendation.message = 'Test account access is working with MCC login header!';
    recommendation.next_steps = [
      'Use MCC login-customer-id header for all requests',
      'Proceed with building automation scripts',
      'Create test campaigns for development'
    ];
    recommendation.optimal_config = {
      use_mcc_header: true,
      mcc_id: mccId,
      test_account_id: testAccountId,
      api_version: 'v16'
    };
  } else if (results.mcc_access?.success && results.customer_hierarchy?.success) {
    // MCC works but test account doesn't
    const targetInHierarchy = results.customer_hierarchy.customers?.find(c => c.is_target);
    
    if (targetInHierarchy) {
      recommendation.status = 'account_linked_but_access_failed';
      recommendation.message = 'Test account is linked to MCC but API access failed.';
      recommendation.next_steps = [
        'Wait 24 hours after account linking',
        'Try using different API version',
        'Check developer token permissions'
      ];
    } else {
      recommendation.status = 'account_not_linked';
      recommendation.message = 'Test account is not properly linked to MCC.';
      recommendation.next_steps = [
        'Re-link test account to MCC',
        'Verify account IDs are correct',
        'Check MCC permissions'
      ];
    }
  } else {
    recommendation.status = 'mcc_access_failed';
    recommendation.message = 'Cannot access MCC account.';
    recommendation.next_steps = [
      'Verify MCC account ID is correct',
      'Check developer token has MCC access',
      'Verify OAuth permissions include Google Ads'
    ];
  }
  
  return recommendation;
}

/**
 * Get account linking status
 * @param {Object} params - Parameters with API client and credentials
 * @returns {Object} Linking status
 */
async function getAccountLinkingStatus({ apiClient, headers, testAccountId, mccId }) {
  try {
    const response = await apiClient.getCustomerHierarchy(mccId, {
      ...headers,
      'login-customer-id': undefined
    });
    
    if (response.data.results && response.data.results.length > 0) {
      const targetClient = response.data.results.find(result => 
        result.customerClient.clientCustomer === testAccountId
      );
      
      if (targetClient) {
        const client = targetClient.customerClient;
        return {
          linked: true,
          account_name: client.descriptiveName,
          is_test: client.testAccount,
          status: 'active' // Google Ads doesn't return status in this query
        };
      }
    }
    
    return { linked: false };
    
  } catch (error) {
    return { 
      linked: false, 
      error: error.response?.data?.error?.message || error.message 
    };
  }
}

module.exports = { 
  debugTestAccountAccess, 
  getAccountLinkingStatus,
  debugCampaignTargeting  // Fixed: added comma
};