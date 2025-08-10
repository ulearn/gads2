const express = require('express');
const mysql = require('mysql2/promise');
const hubspot = require('@hubspot/api-client');
const { google } = require('googleapis');
const { GoogleAdsApi } = require('google-ads-api');
require('dotenv').config();

// Set up logging with auto-rotation
const logger = require('./logger');
logger.setupLogger();

const app = express();
const router = express.Router();

// Use router for all your routes, then mount it
// Use environment variable for base path, fallback to /gads
const BASE_PATH = process.env.BASE_PATH || '/gads';
app.use(BASE_PATH, router);
const PORT = process.env.PORT || 8080;

// Middleware
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true }));

// Request logging
app.use((req, res, next) => {
  console.log(`${req.method} ${req.path} - ${req.ip}`);
  next();
});

//=============================================================================//
//   CENTRALIZED API CLIENT SETUP
//=============================================================================//

// HubSpot Client - Initialized once
const hubspotClient = new hubspot.Client({ 
  accessToken: process.env.HubAccess 
});

// Google Ads OAuth Client - Initialized once  
const googleOAuth = new google.auth.OAuth2(
  process.env.CLIENT_ID,
  process.env.CLIENT_SECRET,
  process.env.REDIRECT_URI
);

// Set refresh token
googleOAuth.setCredentials({
  refresh_token: process.env.GOOGLE_REFRESH_TOKEN,
});

// Google Ads API Client initialization
async function initializeGoogleAdsClient() {
  try {
    console.log('🔧 Initializing Google Ads client...');
    
    // Check required environment variables
    const requiredVars = ['CLIENT_ID', 'CLIENT_SECRET', 'GAdsAPI', 'GOOGLE_REFRESH_TOKEN', 'GADS_TEST_ID'];
    const missing = requiredVars.filter(v => !process.env[v]);
    
    if (missing.length > 0) {
      console.error('❌ Missing environment variables:', missing.join(', '));
      return null;
    }
    
    // Clean up customer ID (remove hyphens if present)
    const customerId = process.env.GADS_TEST_ID.replace(/-/g, '');
    const managerId = process.env.GADS_TEST_MCC_ID ? process.env.GADS_TEST_MCC_ID.replace(/-/g, '') : undefined;
    
    console.log('   📊 Creating customer instance...');
    console.log(`      Customer ID: ${customerId}`);
    if (managerId) {
      console.log(`      Manager ID: ${managerId}`);
    }
    
    // Create the Google Ads client with all credentials
    const client = new GoogleAdsApi({
      client_id: process.env.CLIENT_ID,
      client_secret: process.env.CLIENT_SECRET,
      developer_token: process.env.GAdsAPI,
    });
    
    // Create the customer instance with authentication
    const customer = client.Customer({
      customer_id: customerId,
      refresh_token: process.env.GOOGLE_REFRESH_TOKEN,
      login_customer_id: managerId // This is important for MCC accounts
    });
    
    if (!customer) {
      console.error('   ❌ Customer object creation failed');
      return null;
    }
    
    console.log('   ✅ Google Ads customer initialized successfully');
    return customer;
    
  } catch (error) {
    console.error('❌ Failed to initialize Google Ads client:', error.message);
    return null;
  }
}

// Database Connection Pool - Reusable
const dbConfig = {
  host: process.env.DB_HOST,
  port: process.env.DB_PORT,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0
};

// Create database connection helper
const getDbConnection = async () => {
  return await mysql.createConnection(dbConfig);
};

//=============================================================================//
//   IMPORT SCRIPTS (they receive the clients, don't create their own)
//=============================================================================//

const hubspotSync = require('./scripts/hubspot/hubspot-sync');
const hubspotTest = require('./scripts/hubspot/hubspot-test');
const googleAdsTest = require('./scripts/google/gads-test');
const countryLoader = require('./scripts/country/load-countries');

//=============================================================================//
//   ROUTES - All authentication handled here, passed to scripts
//=============================================================================//

// Root dashboard
router.get('/', (req, res) => {
  res.send(`
    <h1>🎯 Google Ads AI Iterator</h1>
    <p><strong>System Status:</strong> Running | <strong>Build:</strong> ${new Date().toISOString()}</p>
    
    <h2>🏥 System Health</h2>
    <p><a href="/gads/health">Health Check</a> | <a href="/gads/test">Environment Test</a> | <a href="/gads/logs">System Logs</a></p>
    
    <h2>📊 HubSpot Integration</h2>
    <p><a href="/gads/hubspot/test">Test Connection</a> | <a href="/gads/hubspot/sync">Sync Data</a> | <a href="/gads/hubspot/status">View Status</a></p>
    
    <h2>🎯 Google Ads Integration</h2>
    <p><a href="/gads/google-ads/test">Test Connection</a> | <a href="/gads/google-ads/campaigns">Campaigns</a> | <a href="/gads/google-ads/budget">Budget Monitor</a></p>
    
<h2>📈 Analytics & Reports</h2>
<p><a href="/gads/dashboard">📊 Google Ads Dashboard</a> | <a href="/gads/analytics/territory">Territory Analysis</a> | <a href="/gads/analytics/pipeline">Pipeline Performance</a> | <a href="/gads/analytics/budget">Budget Tracking</a></p>
    
    <h2>🛠️ Admin Tools</h2>
    <p><a href="/gads/admin/load-countries">Load Country Data</a> | <a href="/gads/setup/oauth">OAuth Setup (One-time)</a></p>
  `);
});

// Health check with API status
router.get('/health', async (req, res) => {
  const health = {
    status: 'healthy',
    timestamp: new Date().toISOString(),
    services: {
      database: 'unknown',
      hubspot: 'unknown', 
      google_ads: 'unknown'
    }
  };

  // Test database
  try {
    const connection = await getDbConnection();
    await connection.execute('SELECT 1');
    await connection.end();
    health.services.database = 'connected';
  } catch (error) {
    health.services.database = 'error';
    health.status = 'degraded';
  }

  // Test HubSpot
  try {
    await hubspotClient.crm.contacts.basicApi.getPage(1);
    health.services.hubspot = 'connected';
  } catch (error) {
    health.services.hubspot = 'error';
    health.status = 'degraded';
  }

  // Test Google OAuth
  try {
    await googleOAuth.refreshAccessToken();
    health.services.google_ads = 'connected';
  } catch (error) {
    health.services.google_ads = 'error';
    health.status = 'degraded';
  }

  res.json(health);
});

// Environment test
router.get('/test', (req, res) => {
  res.json({
    timestamp: new Date().toISOString(),
    api_clients: {
      hubspot_token: process.env.HubAccess ? 'Present' : 'Missing',
      google_client_id: process.env.CLIENT_ID ? 'Present' : 'Missing',
      google_refresh_token: process.env.GOOGLE_REFRESH_TOKEN ? 'Present' : 'Missing',
      google_dev_token: process.env.GAdsAPI ? 'Present' : 'Missing',
      gads_test_mcc_id: process.env.GADS_TEST_MCC_ID ? 'Present' : 'Missing',
      gads_test_id: process.env.GADS_TEST_ID ? 'Present' : 'Missing'
    },
    database: {
      host: process.env.DB_HOST || 'Missing',
      name: process.env.DB_NAME || 'Missing',
      user: process.env.DB_USER ? 'Present' : 'Missing'
    },
    logging: logger.getLogStats()
  });
});

//=============================================================================// 
//== HUBSPOT ROUTES - Pass authenticated client to scripts
//=============================================================================//
// Add this route to your index.js to test the report without syncing:

router.get('/hubspot/test-report', async (req, res) => {
  try {
    console.log('🔍 Testing report generation...');
    
    // Import the models
    const ContactModel = require('./scripts/models/hub-contact');
    const CountryModel = require('./scripts/models/hub-country');
    
    // Create model instances
    const contactModel = new ContactModel(getDbConnection);
    const countryModel = new CountryModel(getDbConnection);
    
    const results = {
      pipeline: null,
      countries: null,
      burnRate: null,
      errors: []
    };
    
    // Test 1: Pipeline stages (this works)
    try {
      console.log('Testing getContactsByPipelineStage...');
      const stageStats = await contactModel.getContactsByPipelineStage();
      results.pipeline = stageStats;
      console.log('✅ Pipeline stages worked');
    } catch (error) {
      console.error('❌ Pipeline stages failed:', error.message);
      results.errors.push({ test: 'pipeline', error: error.message });
    }
    
    // Test 2: Top Countries (this is failing)
    try {
      console.log('Testing getTopCountries...');
      const countryStats = await contactModel.getTopCountries(30, 5); // Just 30 days, 5 countries
      results.countries = countryStats;
      console.log('✅ Top countries worked');
    } catch (error) {
      console.error('❌ Top countries failed:', error.message);
      results.errors.push({ test: 'countries', error: error.message });
    }
    
    // Test 3: Territory Burn Rate (might also fail)
    try {
      console.log('Testing getTerritoryBurnRate...');
      const burnStats = await contactModel.getTerritoryBurnRate(30); // 30 days
      results.burnRate = burnStats;
      console.log('✅ Territory burn rate worked');
    } catch (error) {
      console.error('❌ Territory burn rate failed:', error.message);
      results.errors.push({ test: 'burnRate', error: error.message });
    }
    
    // Return results
    res.json({
      success: results.errors.length === 0,
      message: results.errors.length === 0 ? 'All tests passed' : `${results.errors.length} test(s) failed`,
      results: results,
      timestamp: new Date().toISOString()
    });
    
  } catch (error) {
    console.error('❌ Test failed:', error.message);
    res.status(500).json({
      success: false,
      error: error.message,
      timestamp: new Date().toISOString()
    });
  }
});


//=============================================================================//
//== HUBSPOT SYNC ROUTES - Pass authenticated client to scripts
//=============================================================================//
// TEST ROUTE fieldmap.js => Add route to index (HubSpot routes section)

router.get('/hubspot/debug-single/:contactId', async (req, res) => {
  try {
    const contactId = req.params.contactId;
    console.log(`🔍 DEBUG: Fetching single contact ${contactId} for detailed analysis...`);
    
    // Get database connection
    const connection = await getDbConnection();
    
    try {
      // Import the debug fieldmap
      const fieldMap = require('./scripts/hubspot/fieldmap');
      
      // Initialize table
      await fieldMap.ensureTableExists(connection, 'contacts');
      
      // Fetch the contact with ALL properties (empty array = all properties)
      console.log(`🔍 DEBUG: Calling HubSpot API for contact ${contactId}...`);
      const contact = await hubspotClient.crm.contacts.basicApi.getById(contactId, []);
      
      console.log(`🔍 DEBUG: Got contact from HubSpot:`, {
        id: contact.id,
        hasProperties: !!contact.properties,
        propertiesCount: contact.properties ? Object.keys(contact.properties).length : 0
      });
      
      // Log specific fields we're looking for
      if (contact.properties) {
        console.log(`🔍 DEBUG: Checking for specific fields:`);
        console.log(`   - hs_object_source_label: ${contact.properties.hs_object_source_label || 'NOT FOUND'}`);
        console.log(`   - hs_object_source_detail_1: ${contact.properties.hs_object_source_detail_1 || 'NOT FOUND'}`);
        console.log(`   - hs_object_source_detail_2: ${contact.properties.hs_object_source_detail_2 || 'NOT FOUND'}`);
        console.log(`   - gclid: ${contact.properties.gclid || 'NOT FOUND'}`);
        console.log(`   - ga_session_id: ${contact.properties.ga_session_id || 'NOT FOUND'}`);
        console.log(`   - ga_client_id: ${contact.properties.ga_client_id || 'NOT FOUND'}`);
      }
      
      // Process the contact using our field mapper
      console.log(`🔍 DEBUG: Processing contact through fieldMap...`);
      const result = await fieldMap.processHubSpotObject(contact, connection, 'contacts');
      
      // Check what columns exist in the database now
      console.log(`🔍 DEBUG: Checking current database columns...`);
      const [columns] = await connection.execute(`
        SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        ORDER BY COLUMN_NAME
      `, [process.env.DB_NAME, 'hub_contacts']);
      
      const dbColumns = columns.map(col => ({
        name: col.COLUMN_NAME,
        type: col.DATA_TYPE,
        nullable: col.IS_NULLABLE
      }));
      
      // Look for our specific columns
      const targetFields = ['hs_object_source_label', 'hs_object_source_detail_1', 'hs_object_source_detail_2', 'gclid', 'ga_session_id', 'ga_client_id'];
      const foundFields = targetFields.filter(field => 
        dbColumns.some(col => col.name === field)
      );
      const missingFields = targetFields.filter(field => 
        !dbColumns.some(col => col.name === field)
      );
      
      res.json({
        success: true,
        contact_id: contactId,
        processing_result: result,
        hubspot_data: {
          total_properties: contact.properties ? Object.keys(contact.properties).length : 0,
          has_target_fields: {
            hs_object_source_label: !!contact.properties?.hs_object_source_label,
            hs_object_source_detail_1: !!contact.properties?.hs_object_source_detail_1,
            hs_object_source_detail_2: !!contact.properties?.hs_object_source_detail_2,
            gclid: !!contact.properties?.gclid,
            ga_session_id: !!contact.properties?.ga_session_id,
            ga_client_id: !!contact.properties?.ga_client_id
          },
          target_field_values: {
            hs_object_source_label: contact.properties?.hs_object_source_label,
            hs_object_source_detail_1: contact.properties?.hs_object_source_detail_1,
            hs_object_source_detail_2: contact.properties?.hs_object_source_detail_2,
            gclid: contact.properties?.gclid,
            ga_session_id: contact.properties?.ga_session_id,
            ga_client_id: contact.properties?.ga_client_id
          }
        },
        database_status: {
          total_columns: dbColumns.length,
          found_target_fields: foundFields,
          missing_target_fields: missingFields,
          all_columns: dbColumns.map(col => col.name)
        },
        timestamp: new Date().toISOString()
      });
      
    } finally {
      await connection.end();
    }
    
  } catch (error) {
    console.error('❌ DEBUG: Single contact debug failed:', error.message);
    res.status(500).json({
      success: false,
      error: error.message,
      stack: error.stack,
      timestamp: new Date().toISOString()
    });
  }
});




// ====================================================================//
// MAIN HUBSPT SYNC ROUTE => /hubspot/sync route - corrected version: //

router.get('/hubspot/sync', async (req, res) => {
  try {
    // Parse query parameters for different sync options
    const { days, start, end, month } = req.query;
    
    let syncOptions = {};
    let description = '';
    
    // Determine sync type from parameters
    if (start && end) {
      // Date range sync
      syncOptions = { startDate: start, endDate: end };
      description = `Date range sync: ${start} to ${end}`;
    } else if (month) {
      // Month sync (format: YYYY-MM)
      const [year, monthNum] = month.split('-');
      const startDate = new Date(year, monthNum - 1, 1);
      const endDate = new Date(year, monthNum, 0); // Last day of month
      syncOptions = { 
        startDate: startDate.toISOString().split('T')[0], 
        endDate: endDate.toISOString().split('T')[0] 
      };
      description = `Month sync: ${month}`;
    } else if (days) {
      // Days back sync (backwards compatibility)
      const daysBack = parseInt(days) || 30;
      syncOptions = { daysBack };  // Pass as object property!
      description = `Sync last ${daysBack} days`;
    } else {
      // Default: last 30 days
      syncOptions = { daysBack: 30 };  // Pass as object property!
      description = 'Default sync (last 30 days)';
    }
    
    console.log(`🔄 Starting HubSpot sync: ${description}`);
    
    // Return immediately, run sync in background
    res.json({
      success: true,
      service: 'HubSpot',
      message: description,
      status: 'running',
      options: syncOptions,
      timestamp: new Date().toISOString()
    });
    
    // Run sync with authenticated clients in background
    (async () => {
      try {
        // IMPORTANT: Pass syncOptions object, not just a number!
// Should call the schema-aware function
const result = await hubspotSync.runSyncWithSchemaCheck(hubspotClient, getDbConnection, syncOptions);
        console.log(`✅ HubSpot sync completed:`, result);
      } catch (error) {
        console.error('❌ Background sync failed:', error.message);
      }
    })();
    
  } catch (error) {
    console.error('❌ HubSpot sync failed:', error.message);
    res.status(500).json({
      success: false,
      service: 'HubSpot',
      error: error.message,
      timestamp: new Date().toISOString()
    });
  }
});

// Add this route to your index.js in the HubSpot section:
router.get('/hubspot/contact/:id', async (req, res) => {
  try {
    const contactId = req.params.id;
    console.log(`🔍 Fetching HubSpot contact ${contactId}...`);
    
    const hubspotTest = require('./scripts/hubspot/hubspot-test');
    const result = await hubspotTest.fetchSpecificContact(hubspotClient, contactId);
    
    // Return summary (full data is saved to file)
    res.json({
      success: true,
      service: 'HubSpot',
      contactId: contactId,
      summary: {
        totalProperties: Object.keys(result.allProperties).length,
        nonNullProperties: Object.entries(result.allProperties)
          .filter(([k, v]) => v !== null && v !== '').length,
        hasDeals: result.associations && result.associations.length > 0,
        dealCount: result.associations ? result.associations.length : 0
      },
      keyData: {
        email: result.allProperties.email,
        name: `${result.allProperties.firstname || ''} ${result.allProperties.lastname || ''}`.trim(),
        country: result.allProperties.country,
        lifecyclestage: result.allProperties.lifecyclestage,
        created: result.allProperties.createdate
      },
      message: `Full data saved to contact-${contactId}-export.json`,
      timestamp: new Date().toISOString()
    });
    
  } catch (error) {
    console.error('❌ Contact fetch failed:', error.message);
    res.status(500).json({
      success: false,
      service: 'HubSpot',
      error: error.message,
      timestamp: new Date().toISOString()
    });
  }
});

//=============================================================================//
//   GOOGLE ADS ROUTES - REFACTORED (No business logic in index.js)
//=============================================================================//

// Google Ads Connection Test Route - Uses gads-test.js module
router.get('/google-ads/test', async (req, res) => {
  try {
    console.log('🔄 Testing Google Ads connection...');
    
    const customer = await initializeGoogleAdsClient();
    
    if (!customer) {
      throw new Error('Failed to initialize Google Ads client');
    }
    
    // Import test module (business logic)
    const googleAdsTest = require('./scripts/google/gads-test');
    
    // Pass authenticated client to test module
    const result = await googleAdsTest.testConnection({
      customer: customer,
      testAccountId: process.env.GADS_TEST_ID,
      mccId: process.env.GADS_TEST_MCC_ID
    });
    
    res.json({
      success: true,
      service: 'Google Ads',
      result: result,
      timestamp: new Date().toISOString()
    });
    
  } catch (error) {
    console.error('❌ Google Ads test failed:', error.message);
    res.status(500).json({
      success: false,
      service: 'Google Ads',
      error: error.message,
      timestamp: new Date().toISOString()
    });
  }
});

// Legacy Campaigns Route - Refactored to use campaign.js module
router.get('/google-ads/campaigns', async (req, res) => {
  try {
    console.log('🔄 Fetching Google Ads campaigns (legacy endpoint)...');
    
    const customer = await initializeGoogleAdsClient();
    if (!customer) {
      throw new Error('Failed to initialize Google Ads client');
    }
    
    // Import campaign module (business logic moved here)
    const campaignModule = require('./scripts/google/campaign');
    
    // Use the new getCampaignPerformance function but format for legacy compatibility
    const result = await campaignModule.getCampaignPerformance(customer, { days: 30 });
    
    if (!result.success) {
      throw new Error(result.error);
    }
    
    // Format response for legacy compatibility
    res.json({
      success: true,
      count: result.campaigns.length,
      campaigns: result.campaigns.map(c => ({
        id: c.id,
        name: c.name,
        status: c.status,
        type: c.type,
        budget: c.budget.daily_amount,
        metrics: {
          impressions: c.metrics.impressions,
          clicks: c.metrics.clicks,
          cost: c.metrics.cost
        }
      })),
      timestamp: new Date().toISOString()
    });
    
  } catch (error) {
    console.error('❌ Failed to fetch campaigns:', error.message);
    res.status(500).json({
      success: false,
      error: error.message,
      timestamp: new Date().toISOString()
    });
  }
});

// Budget Route - Refactored to use budget.js module
router.get('/google-ads/budget', async (req, res) => {
  try {
    console.log('🔄 Fetching budget information...');
    
    const customer = await initializeGoogleAdsClient();
    if (!customer) {
      throw new Error('Failed to initialize Google Ads client');
    }
    
    // Import budget module (we'll create this)
    const budgetModule = require('./scripts/google/budget');
    
    // Pass authenticated client to budget module
    const result = await budgetModule.getBudgetInfo(customer);
    
    res.json(result);
    
  } catch (error) {
    console.error('❌ Failed to fetch budgets:', error.message);
    res.status(500).json({
      success: false,
      error: error.message,
      timestamp: new Date().toISOString()
    });
  }
});

//=============================================================================//
//   NEW ENHANCED GOOGLE ADS ROUTES - Using dedicated modules
//=============================================================================//
router.get('/google-ads/campaigns/targeting', async (req, res) => {
  try {
    console.log('🎯 Fetching campaign targeting data...');
    
    // Initialize the Google Ads client - THIS WAS MISSING!
    const customer = await initializeGoogleAdsClient();
    if (!customer) {
      throw new Error('Failed to initialize Google Ads client');
    }
    
    const campaignModule = require('./scripts/google/campaign');
    console.log('📦 Available module functions:', Object.keys(campaignModule));
    
    const campaignId = '22873198957';
    console.log(`🎯 Using campaign ID: ${campaignId}`);
    
    const result = await campaignModule.getCampaignTargeting(customer, campaignId);
    res.json(result);
    
  } catch (error) {
    console.error('❌ Campaign targeting failed:', error.message);
    res.status(500).json({ success: false, error: error.message });
  }
});

// Enhanced Campaign Performance Route
router.get('/google-ads/campaigns/performance', async (req, res) => {
  try {
    console.log('🎯 Fetching detailed campaign performance...');
    
    const customer = await initializeGoogleAdsClient();
    if (!customer) {
      throw new Error('Failed to initialize Google Ads client');
    }
    
    // Import campaign module
    const campaignModule = require('./scripts/google/campaign');
    
    // Parse query parameters (no business logic here)
    const options = {};
    if (req.query.days) options.days = parseInt(req.query.days);
    if (req.query.start && req.query.end) {
      options.startDate = req.query.start;
      options.endDate = req.query.end;
    }
    
    // Delegate to module
    const result = await campaignModule.getCampaignPerformance(customer, options);
    res.json(result);
    
  } catch (error) {
    console.error('❌ Campaign performance failed:', error.message);
    res.status(500).json({
      success: false,
      error: error.message,
      timestamp: new Date().toISOString()
    });
  }
});

// Campaign Targeting Route
router.get('/google-ads/campaigns/targeting', async (req, res) => {
  try {
    console.log('🎯 Fetching campaign targeting data...');
    
    const customer = await initializeGoogleAdsClient();
    if (!customer) {
      throw new Error('Failed to initialize Google Ads client');
    }
    
    const campaignModule = require('./scripts/google/campaign');
    const campaignId = req.query.campaign_id || null;
    const result = await campaignModule.getCampaignTargeting(customer, campaignId);
    res.json(result);
    
  } catch (error) {
    console.error('❌ Campaign targeting failed:', error.message);
    res.status(500).json({
      success: false,
      error: error.message,
      timestamp: new Date().toISOString()
    });
  }
});

// Campaign Keywords Route
router.get('/google-ads/campaigns/keywords', async (req, res) => {
  try {
    console.log('🔍 Fetching campaign keywords and search terms...');
    
    const customer = await initializeGoogleAdsClient();
    if (!customer) {
      throw new Error('Failed to initialize Google Ads client');
    }
    
    // Import campaign module
    const campaignModule = require('./scripts/google/campaign');
    
    // Parse parameters (no business logic here)
    const options = {};
    if (req.query.days) options.days = parseInt(req.query.days);
    
    // Delegate to module
    const result = await campaignModule.getCampaignKeywords(customer, options);
    res.json(result);
    
  } catch (error) {
    console.error('❌ Campaign keywords failed:', error.message);
    res.status(500).json({
      success: false,
      error: error.message,
      timestamp: new Date().toISOString()
    });
  }
});

// Updated Burn Rate Analysis Route => burn.js
router.get('/google-ads/campaigns/burn-rate', async (req, res) => {
  try {
    console.log('🔥 Analyzing campaign burn rate...');
    
    const customer = await initializeGoogleAdsClient();
    if (!customer) {
      throw new Error('Failed to initialize Google Ads client');
    }
    
    // Import required modules
    const campaignModule = require('./scripts/google/campaign');
    const hubspotData = require('./scripts/analytics/hubspot-data');
    
    // Parse parameters (no business logic here)
    const days = parseInt(req.query.days) || 30;
    
    // Delegate to modules and combine results
    const [targetingResult, territoryResult] = await Promise.all([
      campaignModule.getCampaignTargeting(customer),
      hubspotData.getTerritoryAnalysis(getDbConnection, days)
    ]);
    
    if (!targetingResult.success || !territoryResult.success) {
      throw new Error('Failed to fetch required data for burn rate analysis');
    }
    
    // Import burn rate analysis module (renamed to burn.js)
    const burnAnalysis = require('./scripts/analytics/burn');
    
    // Delegate complex analysis to dedicated module
    const result = await burnAnalysis.analyzeBurnRate({
      campaigns: targetingResult.campaigns,
      territories: territoryResult.territories,
      burnRateSummary: territoryResult.burnRateSummary,
      days: days
    });
    
    res.json(result);
    
  } catch (error) {
    console.error('❌ Burn rate analysis failed:', error.message);
    res.status(500).json({
      success: false,
      error: error.message,
      timestamp: new Date().toISOString()
    });
  }
});

//=============================================================================//
//   ANALYTICS ROUTES - Use database connection
//=============================================================================//
// React Dashboard Route
router.get('/dashboard', (req, res) => {
  const dashboardServer = require('./scripts/analytics/dashboard-server');
  dashboardServer.serveDashboard(req, res);
});

// HubSpot Dashboard Data API
router.get('/api/dashboard-data', async (req, res) => {
  try {
    const days = parseInt(req.query.days) || 30;
    const hubspotData = require('./scripts/analytics/hubspot-data');
    const result = await hubspotData.getDashboardSummary(getDbConnection, days);
    res.json(result);
  } catch (error) {
    console.error('❌ Dashboard data API failed:', error.message);
    res.status(500).json({ error: error.message });
  }
});

// Campaign Performance API
router.get('/api/campaigns', async (req, res) => {
  try {
    const days = parseInt(req.query.days) || 30;
    const hubspotData = require('./scripts/analytics/hubspot-data');
    const result = await hubspotData.getCampaignPerformance(getDbConnection, days);
    res.json(result);
  } catch (error) {
    console.error('❌ Campaign data API failed:', error.message);
    res.status(500).json({ error: error.message });
  }
});

// Trend Data API
router.get('/api/trends', async (req, res) => {
  try {
    const days = parseInt(req.query.days) || 30;
    const hubspotData = require('./scripts/analytics/hubspot-data');
    const result = await hubspotData.getTrendData(getDbConnection, days);
    res.json(result);
  } catch (error) {
    console.error('❌ Trend data API failed:', error.message);
    res.status(500).json({ error: error.message });
  }
});

// Territory Analysis API
router.get('/api/territories', async (req, res) => {
  try {
    const days = parseInt(req.query.days) || 30;
    const hubspotData = require('./scripts/analytics/hubspot-data');
    const result = await hubspotData.getTerritoryAnalysis(getDbConnection, days);
    res.json(result);
  } catch (error) {
    console.error('❌ Territory data API failed:', error.message);
    res.status(500).json({ error: error.message });
  }
});

// Territory Analysis
router.get('/analytics/territory', async (req, res) => {
  try {
    const analyticsModule = require('./scripts/analytics/hubspot-data');
    const days = parseInt(req.query.days) || 30;
    const result = await analyticsModule.getTerritoryAnalysis(getDbConnection, days);
    res.json(result);
  } catch (error) {
    console.error('❌ Territory analysis failed:', error.message);
    res.status(500).json({
      error: error.message,
      timestamp: new Date().toISOString()
    });
  }
});

// Pipeline Analysis  
router.get('/analytics/pipeline', async (req, res) => {
  res.json({
    success: true,
    message: 'Pipeline analytics endpoint - coming soon',
    note: 'This will show Google Ads → HubSpot pipeline correlation',
    timestamp: new Date().toISOString()
  });
});

// Budget Analytics Route
router.get('/analytics/budget', async (req, res) => {
  try {
    const customer = await initializeGoogleAdsClient();
    const analyticsModule = require('./scripts/analytics/budget');
    const result = await analyticsModule.getBudgetPerformanceAnalysis(
      getDbConnection, customer, { days: parseInt(req.query.days) || 30 }
    );
    res.json(result);
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

//=============================================================================//
//   ADMIN ROUTES - Database and setup tools
//=============================================================================//

router.get('/admin/load-countries', async (req, res) => {
  try {
    console.log('🔄 Loading country data...');
    const result = await countryLoader.loadCountryData(getDbConnection);
    
    res.json({
      success: true,
      message: 'Country data loaded successfully',
      result: result,
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    console.error('❌ Country loading failed:', error.message);
    res.status(500).json({
      success: false,
      error: error.message,
      timestamp: new Date().toISOString()
    });
  }
});

// One-time OAuth setup (legacy)
router.get('/setup/oauth', (req, res) => {
  const code = req.query.code;

  if (!code) {
    const url = googleOAuth.generateAuthUrl({
      access_type: 'offline',
      prompt: 'consent',
      scope: [
        'https://www.googleapis.com/auth/adwords',
        'https://www.googleapis.com/auth/userinfo.email',
        'https://www.googleapis.com/auth/userinfo.profile',
      ],
    });

    return res.send(`
      <h2>🔐 Google OAuth Setup (One-time)</h2>
      <p><a href="${url}">Authorize Google Ads API Access</a></p>
      <p><a href="/gads/">← Back to Dashboard</a></p>
    `);
  }

  googleOAuth.getToken(code)
    .then(({ tokens }) => {
      if (tokens.refresh_token) {
        console.log(`✅ OAuth refresh token obtained`);
        res.send(`
          <h2>✅ OAuth Setup Complete</h2>
          <h3>Refresh Token:</h3>
          <textarea rows="3" cols="80" readonly>${tokens.refresh_token}</textarea>
          <p><strong>Save this in your .env file as GOOGLE_REFRESH_TOKEN</strong></p>
          <p><a href="/gads/">← Back to Dashboard</a></p>
        `);
      } else {
        res.send(`
          <h2>⚠️ No Refresh Token</h2>
          <p>Remove app permissions and try again.</p>
          <p><a href="/gads/setup/oauth">Try Again</a></p>
        `);
      }
    })
    .catch(err => {
      console.error('❌ OAuth error:', err.message);
      res.status(500).send(`<h2>❌ Error: ${err.message}</h2>`);
    });
});

// Logs endpoint
router.get('/logs', (req, res) => {
  try {
    const fs = require('fs');
    const logFile = './gads.log';
    
    if (!fs.existsSync(logFile)) {
      return res.json({
        message: 'No log file found yet',
        timestamp: new Date().toISOString()
      });
    }
    
    const logs = fs.readFileSync(logFile, 'utf8');
    const lines = logs.split('\n').filter(line => line.trim()).slice(-100);
    
    res.json({
      recent_logs: lines,
      log_stats: logger.getLogStats(),
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    console.error('❌ Log retrieval failed:', error.message);
    res.status(500).json({
      error: error.message,
      timestamp: new Date().toISOString()
    });
  }
});

// Error handling
router.use((error, req, res, next) => {
  console.error('❌ Unhandled error:', error.message);
  res.status(500).json({
    error: 'Internal server error',
    message: error.message,
    timestamp: new Date().toISOString()
  });
});

// 404 handler
router.use((req, res) => {
  res.status(404).json({
    error: 'Endpoint not found',
    path: req.path,
    timestamp: new Date().toISOString()
  });
});

// Start server
app.listen(PORT, () => {
  console.log(`🎉 Google Ads AI Iterator started on port ${PORT}`);
  console.log(`📊 Dashboard: https://hub.ulearnschool.com/gads/`);
  console.log(`🏥 Health: https://hub.ulearnschool.com/gads/health`);
  console.log('');
  console.log('✅ Centralized API clients initialized:');
  console.log('   📊 HubSpot Client: Ready');
  console.log('   🎯 Google OAuth: Ready');  
  console.log('   🎯 Google Ads API Client: Ready');
  console.log('   🗄️  Database Pool: Ready');
});

module.exports = app;