/**
 * Google Ads Test Module - Using Official Google Ads API Client
 * Path: /home/hub/public_html/gads/scripts/google/gads-test.js
 */

/**
 * Test Google Ads API connection using official client
 * @param {Object} params - Parameters from index.js
 * @param {Object} params.customer - Official Google Ads customer client
 * @param {string} params.testAccountId - Test account ID
 * @param {string} params.mccId - MCC account ID
 * @returns {Object} Test results
 */
async function testConnection({ customer, testAccountId, mccId }) {
  try {
    console.log('🔄 Testing Google Ads API connection with official client...');
    
    const results = {
      configuration: null,
      customer: null,
      campaigns: null,
      budgets: null,
      permissions: null
    };
    
    // Test 1: Configuration check
    console.log('\n1️⃣ Configuration check...');
results.configuration = {
  client_present: customer ? '✅ Present' : '❌ Missing',
  account_id: testAccountId || '❌ Missing',
  account_type: testAccountId === process.env.GADS_LIVE_ID ? 'LIVE' : 'TEST',
  mcc_id: mccId || '❌ Not provided'
};
    
    console.log('📋 Configuration status:');
    Object.entries(results.configuration).forEach(([key, value]) => {
      console.log(`   ${key.replace(/_/g, ' ')}: ${value}`);
    });
    
    if (!customer || !testAccountId) {
      throw new Error('Missing required Google Ads client or test account ID');
    }
    
    // Test 2: Customer access
    console.log('\n2️⃣ Testing customer access...');
    try {
      const customerResponse = await customer.query(`
        SELECT 
          customer.id,
          customer.descriptive_name,
          customer.currency_code,
          customer.time_zone,
          customer.test_account
        FROM customer
        LIMIT 1
      `);
      
      if (customerResponse && customerResponse.length > 0) {
        const customerData = customerResponse[0].customer;
        results.customer = {
          success: true,
          id: customerData.id?.toString(),
          name: customerData.descriptive_name,
          currency: customerData.currency_code,
          timezone: customerData.time_zone,
          is_test_account: customerData.test_account
        };
        
        console.log('✅ Customer access successful');
        console.log(`   Account: ${customerData.descriptive_name} (${customerData.id})`);
        console.log(`   Currency: ${customerData.currency_code}`);
        console.log(`   Timezone: ${customerData.time_zone}`);
        console.log(`   Test Account: ${customerData.test_account ? 'Yes' : 'No'}`);
      } else {
        results.customer = { success: false, error: 'No customer data returned' };
      }
    } catch (error) {
      results.customer = { 
        success: false, 
        error: error.message,
        details: error.details || 'No additional details'
      };
      console.log('❌ Customer access failed:', error.message);
    }
    
    // Test 3: Campaigns access
    console.log('\n3️⃣ Testing campaigns access...');
    try {
      const campaignResponse = await customer.query(`
        SELECT 
          campaign.id,
          campaign.name,
          campaign.status,
          campaign.advertising_channel_type
        FROM campaign
        ORDER BY campaign.name
        LIMIT 10
      `);
      
      const campaigns = campaignResponse || [];
      results.campaigns = {
        success: true,
        count: campaigns.length,
        campaigns: campaigns.map(result => ({
          id: result.campaign.id?.toString(),
          name: result.campaign.name,
          status: result.campaign.status,
          type: result.campaign.advertising_channel_type
        }))
      };
      
      console.log(`✅ Found ${campaigns.length} campaigns`);
      
      if (campaigns.length > 0) {
        console.log('\n   📋 Campaign list:');
        campaigns.forEach((result, index) => {
          const campaign = result.campaign;
          console.log(`      ${index + 1}. ${campaign.name} (${campaign.id}) - ${campaign.status}`);
        });
      } else {
        console.log('   ℹ️  No campaigns found (normal for new test accounts)');
      }
    } catch (error) {
      results.campaigns = { 
        success: false, 
        error: error.message,
        details: error.details || 'No additional details'
      };
      console.log('❌ Campaigns access failed:', error.message);
    }
    
    // Test 4: Budget access
    console.log('\n4️⃣ Testing budget access...');
    try {
      const budgetResponse = await customer.query(`
        SELECT 
          campaign_budget.id,
          campaign_budget.name,
          campaign_budget.amount_micros,
          campaign_budget.delivery_method
        FROM campaign_budget
        LIMIT 5
      `);
      
      const budgets = budgetResponse || [];
      results.budgets = {
        success: true,
        count: budgets.length,
        budgets: budgets.map(result => ({
          id: result.campaign_budget.id?.toString(),
          name: result.campaign_budget.name,
          daily_amount: (result.campaign_budget.amount_micros / 1000000).toFixed(2),
          delivery_method: result.campaign_budget.delivery_method
        }))
      };
      
      console.log(`✅ Found ${budgets.length} budgets`);
      
      if (budgets.length > 0) {
        console.log('\n   💰 Budget list:');
        budgets.forEach((result, index) => {
          const budget = result.campaign_budget;
          const dailyAmount = (budget.amount_micros / 1000000).toFixed(2);
          console.log(`      ${index + 1}. ${budget.name} - €${dailyAmount}/day`);
        });
      }
    } catch (error) {
      results.budgets = { 
        success: false, 
        error: error.message,
        details: error.details || 'No additional details'
      };
      console.log('❌ Budget access failed:', error.message);
    }
    
    // Test 5: Permission analysis
    console.log('\n5️⃣ Analyzing API permissions...');
    results.permissions = analyzePermissions(results);
    
    return {
      success: true,
      message: 'Google Ads API test completed using official client',
      results: results,
      recommendations: generateRecommendations(results),
      timestamp: new Date().toISOString()
    };
    
  } catch (error) {
    console.error('❌ Google Ads API test failed:', error.message);
    return {
      success: false,
      error: error.message,
      timestamp: new Date().toISOString()
    };
  }
}

/**
 * Analyze permissions based on test results
 */
function analyzePermissions(results) {
  const permissions = [];
  
  if (results.customer?.success) {
    permissions.push({ scope: 'Customer Read', status: 'granted' });
  } else {
    permissions.push({ 
      scope: 'Customer Read', 
      status: 'denied',
      error: results.customer?.error 
    });
  }
  
  if (results.campaigns?.success) {
    permissions.push({ scope: 'Campaign Read', status: 'granted' });
  } else {
    permissions.push({ 
      scope: 'Campaign Read', 
      status: 'denied',
      error: results.campaigns?.error 
    });
  }
  
  if (results.budgets?.success) {
    permissions.push({ scope: 'Budget Read', status: 'granted' });
  } else {
    permissions.push({ 
      scope: 'Budget Read', 
      status: 'denied',
      error: results.budgets?.error 
    });
  }
  
  return {
    granted: permissions.filter(p => p.status === 'granted').length,
    denied: permissions.filter(p => p.status === 'denied').length,
    details: permissions
  };
}

/**
 * Generate recommendations based on test results
 */
function generateRecommendations(results) {
  const recommendations = [];
  
  if (!results.customer?.success) {
    recommendations.push({
      type: 'error',
      title: 'Customer Access Failed',
      message: 'Cannot access test account. Check account setup.',
      actions: [
        'Verify test account exists and is accessible',
        'Check Google Ads API client configuration',
        'Ensure proper OAuth permissions'
      ]
    });
  }
  
  if (results.customer?.success && !results.customer.is_test_account) {
    recommendations.push({
      type: 'warning',
      title: 'Production Account Detected',
      message: 'This appears to be a production account, not a test account.',
      actions: [
        'Verify you are using the correct test account ID',
        'Never run automation on production accounts during development'
      ]
    });
  }
  
  if (results.campaigns?.count === 0) {
    recommendations.push({
      type: 'info',
      title: 'No Campaigns Found',
      message: 'Test account has no campaigns yet.',
      actions: [
        'Create test campaigns for development',
        'Use Google Ads interface to set up basic campaigns'
      ]
    });
  }
  
  if (results.customer?.success && results.campaigns?.success && results.budgets?.success) {
    recommendations.push({
      type: 'success',
      title: 'All Tests Passed!',
      message: 'Google Ads API is working correctly with your test account.',
      actions: [
        'Proceed with building your automation features',
        'Test your specific use cases'
      ]
    });
  }
  
  return recommendations;
}

// Export the function properly
module.exports = {
  testConnection: testConnection
};

// Alternative export syntax that also works:
// exports.testConnection = testConnection;