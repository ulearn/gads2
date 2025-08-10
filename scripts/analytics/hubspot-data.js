/**
 * HubSpot Dashboard Data API - SCHEMA CORRECTED
 * /scripts/analytics/hubspot-data.js
 * 
 * FIXES:
 * - Corrected join: hub_contacts.hubspot_id = hub_deals.hubspot_deal_id (WRONG)
 * - Should use: hub_contact_deal_associations table for proper joins
 * - OR: Find actual contact reference field in hub_deals
 * - Fixed all SQL queries to match actual schema
 */

const fs = require('fs');
const path = require('path');

// Cache for country data to avoid reading file repeatedly
let countryDataCache = null;

/**
 * Load country classifications from reference file synchronously
 */
function loadCountryClassifications() {
  if (countryDataCache) {
    return countryDataCache;
  }
  
  try {
    const countryFilePath = path.join(__dirname, '../country/country-codes.json');
    console.log(`📚 Loading country data from: ${countryFilePath}`);
    
    const countryData = fs.readFileSync(countryFilePath, 'utf8');
    const countriesMap = JSON.parse(countryData);
    
    // Extract just the unsupported territories
    const unsupportedTerritories = [];
    
    for (const [countryName, countryInfo] of Object.entries(countriesMap)) {
      if (countryInfo.territory === 'Unsupported Territory') {
        unsupportedTerritories.push(countryName);
      }
    }
    
    countryDataCache = unsupportedTerritories;
    console.log(`📚 Loaded ${unsupportedTerritories.length} unsupported territories from reference file`);
    
    return unsupportedTerritories;
    
  } catch (error) {
    console.error('❌ Error loading country classifications:', error.message);
    console.error('❌ Falling back to empty unsupported list');
    return [];
  }
}

/**
 * Robust Google Ads attribution logic
 * Uses multiple HubSpot fields to identify Google Ads contacts
 */
function buildGoogleAdsAttributionQuery() {
  return `(
    gclid IS NOT NULL AND gclid != '' 
    OR hs_analytics_source = 'PAID_SEARCH'
    OR hs_object_source_label LIKE '%Google Ads%'
    OR hs_object_source_label LIKE '%google%'
    OR hs_analytics_first_touch_converting_campaign IS NOT NULL
    OR hs_analytics_last_touch_converting_campaign IS NOT NULL
  )`;
}

/**
 * MOVED FROM INDEX: Google Ads Attribution Test
 */
async function testGoogleAdsAttribution(getDbConnection, days = 7) {
  try {
    const connection = await getDbConnection();
    
    try {
      const endDate = new Date();
      const startDate = new Date();
      startDate.setDate(startDate.getDate() - days);
      
      const startDateStr = startDate.toISOString().slice(0, 19).replace('T', ' ');
      const endDateStr = endDate.toISOString().slice(0, 19).replace('T', ' ');
      
      // Test each attribution method
      const attributionQuery = buildGoogleAdsAttributionQuery();
      
      const [attributionResult] = await connection.execute(`
        SELECT 
          COUNT(*) as total_contacts,
          COUNT(CASE WHEN gclid IS NOT NULL AND gclid != '' THEN 1 END) as has_gclid,
          COUNT(CASE WHEN hs_analytics_source = 'PAID_SEARCH' THEN 1 END) as paid_search,
          COUNT(CASE WHEN hs_object_source_label LIKE '%Google Ads%' THEN 1 END) as google_ads_label,
          COUNT(CASE WHEN hs_analytics_first_touch_converting_campaign IS NOT NULL THEN 1 END) as first_touch_campaign,
          COUNT(CASE WHEN hs_analytics_last_touch_converting_campaign IS NOT NULL THEN 1 END) as last_touch_campaign,
          COUNT(CASE WHEN ${attributionQuery} THEN 1 END) as google_ads_attributed
        FROM hub_contacts 
        WHERE createdate >= ? AND createdate <= ?
      `, [startDateStr, endDateStr]);
      
      const result = attributionResult[0];
      
      return {
        success: true,
        attribution_test: {
          date_range: { start: startDateStr, end: endDateStr, days },
          total_contacts: parseInt(result.total_contacts) || 0,
          attribution_methods: {
            gclid_field: parseInt(result.has_gclid) || 0,
            paid_search_source: parseInt(result.paid_search) || 0,
            google_ads_label: parseInt(result.google_ads_label) || 0,
            first_touch_campaign: parseInt(result.first_touch_campaign) || 0,
            last_touch_campaign: parseInt(result.last_touch_campaign) || 0
          },
          google_ads_attributed_total: parseInt(result.google_ads_attributed) || 0,
          attribution_coverage: result.total_contacts > 0 
            ? ((result.google_ads_attributed / result.total_contacts) * 100).toFixed(1) + '%'
            : '0%'
        },
        timestamp: new Date().toISOString()
      };
      
    } finally {
      await connection.end();
    }
    
  } catch (error) {
    console.error('❌ Attribution test failed:', error.message);
    return {
      success: false,
      error: error.message,
      timestamp: new Date().toISOString()
    };
  }
}

/**
 * FIXED: Get MQL to SQL validation metrics using proper schema
 */
async function getMQLValidationMetrics(getDbConnection, days = 30, analysisMode = 'pipeline') {
  try {
    const connection = await getDbConnection();
    
    try {
      const endDate = new Date();
      const startDate = new Date();
      startDate.setDate(startDate.getDate() - days);
      
      const startDateStr = startDate.toISOString().slice(0, 19).replace('T', ' ');
      const endDateStr = endDate.toISOString().slice(0, 19).replace('T', ' ');
      
      console.log(`🎯 Getting MQL validation metrics for ${days} days (${analysisMode} mode)...`);
      
      // Load unsupported territories
      const unsupportedTerritories = loadCountryClassifications();
      
      // Step 1: Count ALL Google Ads contacts (MQLs)
      const googleAdsAttributionQuery = buildGoogleAdsAttributionQuery();
      
      const [mqlContactsResult] = await connection.execute(`
        SELECT 
          COUNT(*) as total_mqls,
          COUNT(CASE WHEN COALESCE(nationality, country, territory, '') IN (${unsupportedTerritories.map(() => '?').join(',') || 'NULL'}) THEN 1 END) as unsupported_mqls,
          COUNT(CASE WHEN COALESCE(nationality, country, territory, '') NOT IN (${unsupportedTerritories.map(() => '?').join(',') || 'NULL'}) 
                     AND COALESCE(nationality, country, territory, '') != '' THEN 1 END) as supported_mqls
        FROM hub_contacts 
        WHERE ${googleAdsAttributionQuery}
          AND createdate >= ? 
          AND createdate <= ?
      `, [
        ...unsupportedTerritories, // For unsupported check
        ...unsupportedTerritories, // For supported check  
        startDateStr, 
        endDateStr
      ]);
      
      // Step 2: Count deals created from Google Ads contacts using association table
      const dateCondition = analysisMode === 'revenue' 
        ? 'd.hs_closed_won_date >= ? AND d.hs_closed_won_date <= ?'
        : 'd.createdate >= ? AND d.createdate <= ?';
        
      const [sqlDealsResult] = await connection.execute(`
        SELECT 
          COUNT(*) as total_deals_from_google_ads,
          COUNT(CASE WHEN d.dealstage = 'closedwon' OR d.hs_is_closed_won = 'true' THEN 1 END) as won_deals,
          COUNT(CASE WHEN d.dealstage = 'closedlost' OR d.hs_is_closed_lost = 'true' THEN 1 END) as lost_deals,
          COUNT(CASE WHEN d.dealstage NOT IN ('closedwon', 'closedlost') 
                     AND d.hs_is_closed_won != 'true' 
                     AND d.hs_is_closed_lost != 'true' THEN 1 END) as active_deals
        FROM hub_deals d
        JOIN hub_contact_deal_associations a ON d.hubspot_deal_id = a.deal_hubspot_id
        JOIN hub_contacts c ON a.contact_hubspot_id = c.hubspot_id
        WHERE ${googleAdsAttributionQuery.replace(/hs_analytics_source/g, 'c.hs_analytics_source')
                                       .replace(/gclid/g, 'c.gclid')
                                       .replace(/hs_object_source_label/g, 'c.hs_object_source_label')
                                       .replace(/hs_analytics_first_touch_converting_campaign/g, 'c.hs_analytics_first_touch_converting_campaign')
                                       .replace(/hs_analytics_last_touch_converting_campaign/g, 'c.hs_analytics_last_touch_converting_campaign')}
          AND ${dateCondition}
      `, [startDateStr, endDateStr]);
      
      const mqlData = mqlContactsResult[0];
      const sqlData = sqlDealsResult[0];
      
      // Step 3: Calculate validation metrics
      const validationRate = mqlData.total_mqls > 0 
        ? ((sqlData.total_deals_from_google_ads / mqlData.total_mqls) * 100).toFixed(1)
        : 0;
        
      const burnRate = mqlData.total_mqls > 0 
        ? ((mqlData.unsupported_mqls / mqlData.total_mqls) * 100).toFixed(1) 
        : 0;
      
      console.log(`✅ MQL Validation: ${mqlData.total_mqls} MQLs → ${sqlData.total_deals_from_google_ads} SQLs (${validationRate}% conversion)`);
      
      return {
        success: true,
        mql_stage: {
          total_mqls: parseInt(mqlData.total_mqls) || 0,
          supported_mqls: parseInt(mqlData.supported_mqls) || 0,
          unsupported_mqls: parseInt(mqlData.unsupported_mqls) || 0,
          burn_rate_percentage: parseFloat(burnRate)
        },
        sql_validation: {
          total_deals_created: parseInt(sqlData.total_deals_from_google_ads) || 0,
          validation_rate_percentage: parseFloat(validationRate),
          active_deals: parseInt(sqlData.active_deals) || 0,
          won_deals: parseInt(sqlData.won_deals) || 0,
          lost_deals: parseInt(sqlData.lost_deals) || 0
        },
        analysis_mode: analysisMode,
        date_range: {
          start: startDateStr,
          end: endDateStr,
          days: days
        },
        timestamp: new Date().toISOString()
      };
      
    } finally {
      await connection.end();
    }
    
  } catch (error) {
    console.error('❌ MQL validation metrics failed:', error.message);
    return {
      success: false,
      error: error.message,
      timestamp: new Date().toISOString()
    };
  }
}

/**
 * FIXED: Get dashboard summary with proper joins
 */
async function getDashboardSummary(getDbConnection, days = 30, analysisMode = 'pipeline') {
  try {
    const connection = await getDbConnection();
    
    try {
      const endDate = new Date();
      const startDate = new Date();
      startDate.setDate(startDate.getDate() - days);
      
      const startDateStr = startDate.toISOString().slice(0, 19).replace('T', ' ');
      const endDateStr = endDate.toISOString().slice(0, 19).replace('T', ' ');
      
      console.log(`📊 Getting dashboard summary for ${days} days (${analysisMode} mode)...`);
      
      // Get MQL validation metrics
      const mqlValidation = await getMQLValidationMetrics(getDbConnection, days, analysisMode);
      
      // Get pipeline stage distribution with proper joins
      const dateCondition = analysisMode === 'revenue' 
        ? 'd.hs_closed_won_date >= ? AND d.hs_closed_won_date <= ?'
        : 'd.createdate >= ? AND d.createdate <= ?';
      
      const [pipelineResult] = await connection.execute(`
        SELECT 
          d.dealstage,
          COUNT(*) as count,
          SUM(CAST(COALESCE(d.amount, '0') as DECIMAL(15,2))) as total_value,
          AVG(CAST(COALESCE(d.amount, '0') as DECIMAL(15,2))) as avg_value
        FROM hub_deals d
        JOIN hub_contact_deal_associations a ON d.hubspot_deal_id = a.deal_hubspot_id
        JOIN hub_contacts c ON a.contact_hubspot_id = c.hubspot_id
        WHERE ${buildGoogleAdsAttributionQuery().replace(/hs_analytics_source/g, 'c.hs_analytics_source')
                                              .replace(/gclid/g, 'c.gclid')
                                              .replace(/hs_object_source_label/g, 'c.hs_object_source_label')
                                              .replace(/hs_analytics_first_touch_converting_campaign/g, 'c.hs_analytics_first_touch_converting_campaign')
                                              .replace(/hs_analytics_last_touch_converting_campaign/g, 'c.hs_analytics_last_touch_converting_campaign')}
          AND ${dateCondition}
        GROUP BY d.dealstage
        ORDER BY count DESC
      `, [startDateStr, endDateStr]);
      
      // Process pipeline stages including LOST stage
      const pipelineStages = {};
      let totalDeals = 0;
      let totalValue = 0;
      let wonDeals = 0;
      let lostDeals = 0;
      
      pipelineResult.forEach(stage => {
        const stageName = stage.dealstage || 'Unknown';
        const count = parseInt(stage.count) || 0;
        const value = parseFloat(stage.total_value) || 0;
        
        pipelineStages[stageName] = {
          count: count,
          value: value,
          avg_value: parseFloat(stage.avg_value) || 0
        };
        
        totalDeals += count;
        totalValue += value;
        
        if (stageName === 'closedwon') wonDeals += count;
        if (stageName === 'closedlost') lostDeals += count;
      });
      
      // Calculate conversion rates
      const conversionRate = mqlValidation.success && mqlValidation.mql_stage.total_mqls > 0
        ? ((wonDeals / mqlValidation.mql_stage.total_mqls) * 100).toFixed(1)
        : 0;
      
      return {
        success: true,
        summary: {
          // MQL Stage
          total_mqls: mqlValidation.success ? mqlValidation.mql_stage.total_mqls : 0,
          territory_validation: mqlValidation.success ? mqlValidation.mql_stage : {},
          
          // SQL Stage
          total_deals: totalDeals,
          active_deals: totalDeals - wonDeals - lostDeals,
          won_deals: wonDeals,
          lost_deals: lostDeals,
          
          // Financial
          total_value: totalValue,
          avg_deal_size: totalDeals > 0 ? (totalValue / totalDeals).toFixed(2) : 0,
          conversion_rate: parseFloat(conversionRate),
          
          // Pipeline breakdown
          pipeline_stages: pipelineStages
        },
        mql_validation_details: mqlValidation.success ? mqlValidation : null,
        analysis_mode: analysisMode,
        period: `Last ${days} days (${analysisMode} analysis)`,
        timestamp: new Date().toISOString()
      };
      
    } finally {
      await connection.end();
    }
    
  } catch (error) {
    console.error('❌ Dashboard summary failed:', error.message);
    return {
      success: false,
      error: error.message,
      timestamp: new Date().toISOString()
    };
  }
}

/**
 * FIXED: Territory analysis with proper schema
 */
async function getTerritoryAnalysis(getDbConnection, days = 30, analysisMode = 'pipeline') {
  try {
    const connection = await getDbConnection();
    
    try {
      const endDate = new Date();
      const startDate = new Date();
      startDate.setDate(startDate.getDate() - days);
      
      const startDateStr = startDate.toISOString().slice(0, 19).replace('T', ' ');
      const endDateStr = endDate.toISOString().slice(0, 19).replace('T', ' ');
      
      console.log(`🌍 Getting territory analysis for ${days} days (${analysisMode} mode)...`);
      
      // Load unsupported territories from reference file
      const unsupportedTerritories = loadCountryClassifications();
      
      // Get territory performance from contacts (MQL level)
      const [territoryContactsResult] = await connection.execute(`
        SELECT 
          CASE 
            WHEN COALESCE(nationality, country, territory, '') IN (${unsupportedTerritories.map(() => '?').join(',') || 'NULL'})
            THEN 'Unsupported Territory'
            ELSE COALESCE(nationality, country, territory, 'Unknown')
          END as territory,
          COUNT(*) as contacts,
          COUNT(CASE WHEN num_associated_deals > 0 THEN 1 END) as deals_created,
          GROUP_CONCAT(DISTINCT COALESCE(nationality, country, territory, '')) as raw_territories
        FROM hub_contacts 
        WHERE ${buildGoogleAdsAttributionQuery()}
          AND createdate >= ? 
          AND createdate <= ?
        GROUP BY CASE 
          WHEN COALESCE(nationality, country, territory, '') IN (${unsupportedTerritories.map(() => '?').join(',') || 'NULL'})
          THEN 'Unsupported Territory'
          ELSE COALESCE(nationality, country, territory, 'Unknown')
        END
        HAVING contacts > 0
        ORDER BY 
          CASE WHEN territory = 'Unsupported Territory' THEN 1 ELSE 2 END,
          contacts DESC
        LIMIT 20
      `, [
        ...unsupportedTerritories, // For territory classification
        startDateStr, 
        endDateStr,
        ...unsupportedTerritories  // For territory classification (repeated)
      ]);
      
      // Get deal progression by territory (SQL level) using association table
      const dateCondition = analysisMode === 'revenue' 
        ? 'd.hs_closed_won_date >= ? AND d.hs_closed_won_date <= ?'
        : 'd.createdate >= ? AND d.createdate <= ?';
        
      const [territoryDealsResult] = await connection.execute(`
        SELECT 
          CASE 
            WHEN COALESCE(c.nationality, c.country, c.territory, '') IN (${unsupportedTerritories.map(() => '?').join(',') || 'NULL'})
            THEN 'Unsupported Territory'
            ELSE COALESCE(c.nationality, c.country, c.territory, 'Unknown')
          END as territory,
          COUNT(*) as deals,
          COUNT(CASE WHEN d.dealstage = 'closedwon' OR d.hs_is_closed_won = 'true' THEN 1 END) as won_deals,
          COUNT(CASE WHEN d.dealstage = 'closedlost' OR d.hs_is_closed_lost = 'true' THEN 1 END) as lost_deals,
          SUM(CAST(COALESCE(d.amount, '0') as DECIMAL(15,2))) as revenue
        FROM hub_deals d
        JOIN hub_contact_deal_associations a ON d.hubspot_deal_id = a.deal_hubspot_id
        JOIN hub_contacts c ON a.contact_hubspot_id = c.hubspot_id
        WHERE ${buildGoogleAdsAttributionQuery().replace(/hs_analytics_source/g, 'c.hs_analytics_source')
                                              .replace(/gclid/g, 'c.gclid')
                                              .replace(/hs_object_source_label/g, 'c.hs_object_source_label')
                                              .replace(/hs_analytics_first_touch_converting_campaign/g, 'c.hs_analytics_first_touch_converting_campaign')
                                              .replace(/hs_analytics_last_touch_converting_campaign/g, 'c.hs_analytics_last_touch_converting_campaign')}
          AND ${dateCondition}
        GROUP BY CASE 
          WHEN COALESCE(c.nationality, c.country, c.territory, '') IN (${unsupportedTerritories.map(() => '?').join(',') || 'NULL'})
          THEN 'Unsupported Territory'
          ELSE COALESCE(c.nationality, c.country, c.territory, 'Unknown')
        END
      `, [
        ...unsupportedTerritories, // For territory classification
        startDateStr, 
        endDateStr,
        ...unsupportedTerritories  // For territory classification (repeated)
      ]);
      
      // Combine contact and deal data
      const territoryMap = new Map();
      
      // Add contact data
      territoryContactsResult.forEach(t => {
        territoryMap.set(t.territory, {
          name: t.territory,
          contacts: parseInt(t.contacts) || 0,
          deals_created: parseInt(t.deals_created) || 0,
          raw_territories: t.raw_territories ? t.raw_territories.split(',') : [],
          deals: 0,
          won_deals: 0,
          lost_deals: 0,
          revenue: 0
        });
      });
      
      // Add deal data
      territoryDealsResult.forEach(t => {
        const territory = territoryMap.get(t.territory);
        if (territory) {
          territory.deals = parseInt(t.deals) || 0;
          territory.won_deals = parseInt(t.won_deals) || 0;
          territory.lost_deals = parseInt(t.lost_deals) || 0;
          territory.revenue = parseFloat(t.revenue) || 0;
        }
      });
      
      // Convert to array and calculate rates
      const territories = Array.from(territoryMap.values()).map((t, index) => {
        const isUnsupported = t.name === 'Unsupported Territory';
        const mqlToSqlRate = t.contacts > 0 ? ((t.deals_created / t.contacts) * 100).toFixed(1) : 0;
        const conversionRate = t.deals > 0 ? ((t.won_deals / t.deals) * 100).toFixed(1) : 0;
        
        return {
          ...t,
          mql_to_sql_rate: parseFloat(mqlToSqlRate),
          conversion_rate: parseFloat(conversionRate),
          isUnsupported: isUnsupported,
          burnRateFlag: isUnsupported && t.contacts > 0,
          color: isUnsupported ? '#EF4444' : getColorByIndex(index)
        };
      });
      
      // Calculate burn rate summary
      const unsupportedTerritory = territories.find(t => t.isUnsupported);
      const totalContacts = territories.reduce((sum, t) => sum + t.contacts, 0);
      const unsupportedContacts = unsupportedTerritory ? unsupportedTerritory.contacts : 0;
      
      console.log(`✅ Territory analysis: ${territories.length} territories, ${unsupportedContacts}/${totalContacts} unsupported`);
      
      return {
        success: true,
        territories: territories,
        burnRateSummary: {
          unsupportedContacts: unsupportedContacts,
          totalContacts: totalContacts,
          burnRatePercentage: totalContacts > 0 ? ((unsupportedContacts / totalContacts) * 100).toFixed(1) : 0
        },
        analysis_mode: analysisMode,
        timestamp: new Date().toISOString()
      };
      
    } finally {
      await connection.end();
    }
    
  } catch (error) {
    console.error('❌ Territory analysis failed:', error.message);
    return {
      success: false,
      error: error.message,
      timestamp: new Date().toISOString()
    };
  }
}

/**
 * Helper function for territory colors
 */
function getColorByIndex(index) {
  const colors = [
    '#3B82F6', '#10B981', '#F59E0B', '#EF4444', '#8B5CF6',
    '#06B6D4', '#84CC16', '#F97316', '#EC4899', '#6366F1'
  ];
  return colors[index % colors.length];
}

/**
 * FIXED: Campaign performance with proper joins
 */
/**
 * FIXED: Campaign performance with proper campaign name extraction
 * This fixes the "Unknown Campaign" and single row issues
 */
async function getCampaignPerformance(getDbConnection, days = 30, analysisMode = 'pipeline') {
  try {
    const connection = await getDbConnection();
    
    try {
      const endDate = new Date();
      const startDate = new Date();
      startDate.setDate(startDate.getDate() - days);
      
      const startDateStr = startDate.toISOString().slice(0, 19).replace('T', ' ');
      const endDateStr = endDate.toISOString().slice(0, 19).replace('T', ' ');
      
      console.log(`🎯 Getting campaign performance for ${days} days (${analysisMode} mode)...`);
      
      // FIXED: Better campaign identification logic
      const dateCondition = analysisMode === 'revenue' 
        ? 'd.hs_closed_won_date >= ? AND d.hs_closed_won_date <= ?'
        : 'd.createdate >= ? AND d.createdate <= ?';
        
      const [campaignResult] = await connection.execute(`
        SELECT 
          CASE 
            WHEN c.hs_analytics_first_touch_converting_campaign IS NOT NULL AND c.hs_analytics_first_touch_converting_campaign != ''
            THEN c.hs_analytics_first_touch_converting_campaign
            WHEN c.hs_analytics_last_touch_converting_campaign IS NOT NULL AND c.hs_analytics_last_touch_converting_campaign != ''
            THEN c.hs_analytics_last_touch_converting_campaign
            WHEN c.hs_object_source_label IS NOT NULL AND c.hs_object_source_label != ''
            THEN c.hs_object_source_label
            WHEN c.hs_analytics_source_data_1 IS NOT NULL AND c.hs_analytics_source_data_1 != ''
            THEN c.hs_analytics_source_data_1
            ELSE 'Direct/Organic'
          END as campaign_name,
          c.hs_analytics_source as source_type,
          COUNT(DISTINCT c.hubspot_id) as contacts,
          COUNT(DISTINCT d.hubspot_deal_id) as deals,
          COUNT(CASE WHEN d.dealstage = 'closedwon' OR d.hs_is_closed_won = 'true' THEN 1 END) as won_deals,
          COUNT(CASE WHEN d.dealstage = 'closedlost' OR d.hs_is_closed_lost = 'true' THEN 1 END) as lost_deals,
          SUM(CAST(COALESCE(d.amount, '0') as DECIMAL(15,2))) as revenue,
          -- Additional campaign info for debugging
          GROUP_CONCAT(DISTINCT c.hs_analytics_source_data_2 SEPARATOR ', ') as keywords,
          GROUP_CONCAT(DISTINCT c.hs_object_source_detail_1 SEPARATOR ', ') as source_details
        FROM hub_contacts c
        LEFT JOIN hub_contact_deal_associations a ON c.hubspot_id = a.contact_hubspot_id
        LEFT JOIN hub_deals d ON a.deal_hubspot_id = d.hubspot_deal_id 
          AND ${dateCondition}
        WHERE ${buildGoogleAdsAttributionQuery().replace(/hs_analytics_source/g, 'c.hs_analytics_source')
                                              .replace(/gclid/g, 'c.gclid')
                                              .replace(/hs_object_source_label/g, 'c.hs_object_source_label')
                                              .replace(/hs_analytics_first_touch_converting_campaign/g, 'c.hs_analytics_first_touch_converting_campaign')
                                              .replace(/hs_analytics_last_touch_converting_campaign/g, 'c.hs_analytics_last_touch_converting_campaign')}
          AND c.createdate >= ? AND c.createdate <= ?
        GROUP BY 
          CASE 
            WHEN c.hs_analytics_first_touch_converting_campaign IS NOT NULL AND c.hs_analytics_first_touch_converting_campaign != ''
            THEN c.hs_analytics_first_touch_converting_campaign
            WHEN c.hs_analytics_last_touch_converting_campaign IS NOT NULL AND c.hs_analytics_last_touch_converting_campaign != ''
            THEN c.hs_analytics_last_touch_converting_campaign
            WHEN c.hs_object_source_label IS NOT NULL AND c.hs_object_source_label != ''
            THEN c.hs_object_source_label
            WHEN c.hs_analytics_source_data_1 IS NOT NULL AND c.hs_analytics_source_data_1 != ''
            THEN c.hs_analytics_source_data_1
            ELSE 'Direct/Organic'
          END,
          c.hs_analytics_source
        HAVING contacts > 0
        ORDER BY contacts DESC
        LIMIT 20
      `, [startDateStr, endDateStr, startDateStr, endDateStr]);
      
      console.log(`📊 Found ${campaignResult.length} campaign groups`);
      
      const campaigns = campaignResult.map((c, index) => ({
        name: c.campaign_name || 'Unknown Source',
        source_type: c.source_type || 'unknown',
        contacts: parseInt(c.contacts) || 0,
        deals: parseInt(c.deals) || 0,
        won_deals: parseInt(c.won_deals) || 0,
        lost_deals: parseInt(c.lost_deals) || 0,
        revenue: parseFloat(c.revenue) || 0,
        keywords: c.keywords || '',
        source_details: c.source_details || '',
        mql_to_sql_rate: c.contacts > 0 ? ((c.deals / c.contacts) * 100).toFixed(1) : 0,
        conversion_rate: c.deals > 0 ? ((c.won_deals / c.deals) * 100).toFixed(1) : 0,
        // Placeholders for Google Ads data integration
        spend: 0,
        clicks: 0,
        impressions: 0,
        cpc: 0,
        roas: c.revenue > 0 ? (c.revenue / 1).toFixed(2) : 0
      }));
      
      console.log(`✅ Campaign performance: ${campaigns.length} campaigns analyzed`);
      console.log(`📋 Sample campaigns:`, campaigns.slice(0, 3).map(c => ({ name: c.name, contacts: c.contacts, deals: c.deals })));
      
      return {
        success: true,
        campaigns: campaigns,
        analysis_mode: analysisMode,
        timestamp: new Date().toISOString()
      };
      
    } finally {
      await connection.end();
    }
    
  } catch (error) {
    console.error('❌ Campaign performance failed:', error.message);
    return {
      success: false,
      error: error.message,
      timestamp: new Date().toISOString()
    };
  }
}

/**
 * FIXED: Territory analysis with proper unsupported territory detection
 * FIXED: Campaign performance with proper campaign name extraction
 * This fixes the "Unknown Campaign" and single row issues
 */
async function getTerritoryAnalysis(getDbConnection, days = 30, analysisMode = 'pipeline') {
  try {
    const connection = await getDbConnection();
    
    try {
      const endDate = new Date();
      const startDate = new Date();
      startDate.setDate(startDate.getDate() - days);
      
      const startDateStr = startDate.toISOString().slice(0, 19).replace('T', ' ');
      const endDateStr = endDate.toISOString().slice(0, 19).replace('T', ' ');
      
      console.log(`🌍 Getting territory analysis for ${days} days (${analysisMode} mode)...`);
      
      // Load unsupported territories from reference file
      const unsupportedTerritories = loadCountryClassifications();
      console.log(`📚 Loaded ${unsupportedTerritories.length} unsupported territories:`, unsupportedTerritories.slice(0, 5));
      
      // FIXED: Better territory classification with more fields
      const [territoryContactsResult] = await connection.execute(`
        SELECT 
          CASE 
            WHEN COALESCE(nationality, country, territory, ip_country, '') IN (${unsupportedTerritories.map(() => '?').join(',') || 'NULL'})
            THEN 'Unsupported Territory'
            ELSE COALESCE(nationality, country, territory, ip_country, 'Unknown')
          END as territory,
          COUNT(*) as contacts,
          COUNT(CASE WHEN num_associated_deals > 0 THEN 1 END) as deals_created,
          GROUP_CONCAT(DISTINCT COALESCE(nationality, country, territory, ip_country, '') SEPARATOR '|') as raw_territories,
          -- Additional debug info
          COUNT(CASE WHEN nationality IS NOT NULL THEN 1 END) as has_nationality,
          COUNT(CASE WHEN country IS NOT NULL THEN 1 END) as has_country,
          COUNT(CASE WHEN territory IS NOT NULL THEN 1 END) as has_territory,
          COUNT(CASE WHEN ip_country IS NOT NULL THEN 1 END) as has_ip_country
        FROM hub_contacts 
        WHERE ${buildGoogleAdsAttributionQuery()}
          AND createdate >= ? 
          AND createdate <= ?
        GROUP BY CASE 
          WHEN COALESCE(nationality, country, territory, ip_country, '') IN (${unsupportedTerritories.map(() => '?').join(',') || 'NULL'})
          THEN 'Unsupported Territory'
          ELSE COALESCE(nationality, country, territory, ip_country, 'Unknown')
        END
        HAVING contacts > 0
        ORDER BY 
          CASE WHEN territory = 'Unsupported Territory' THEN 1 ELSE 2 END,
          contacts DESC
        LIMIT 25
      `, [
        ...unsupportedTerritories, // For territory classification
        startDateStr, 
        endDateStr,
        ...unsupportedTerritories  // For territory classification (repeated)
      ]);
      
      console.log(`📊 Found ${territoryContactsResult.length} territory groups`);
      
      // Log territory breakdown for debugging
      territoryContactsResult.forEach(t => {
        console.log(`🌍 Territory: ${t.territory} - ${t.contacts} contacts (${t.deals_created} with deals)`);
        if (t.territory === 'Unsupported Territory') {
          console.log(`🔥 Unsupported territories found: ${t.raw_territories}`);
        }
      });
      
      // Get deal progression by territory using association table
      const dateCondition = analysisMode === 'revenue' 
        ? 'd.hs_closed_won_date >= ? AND d.hs_closed_won_date <= ?'
        : 'd.createdate >= ? AND d.createdate <= ?';
        
      const [territoryDealsResult] = await connection.execute(`
        SELECT 
          CASE 
            WHEN COALESCE(c.nationality, c.country, c.territory, c.ip_country, '') IN (${unsupportedTerritories.map(() => '?').join(',') || 'NULL'})
            THEN 'Unsupported Territory'
            ELSE COALESCE(c.nationality, c.country, c.territory, c.ip_country, 'Unknown')
          END as territory,
          COUNT(*) as deals,
          COUNT(CASE WHEN d.dealstage = 'closedwon' OR d.hs_is_closed_won = 'true' THEN 1 END) as won_deals,
          COUNT(CASE WHEN d.dealstage = 'closedlost' OR d.hs_is_closed_lost = 'true' THEN 1 END) as lost_deals,
          SUM(CAST(COALESCE(d.amount, '0') as DECIMAL(15,2))) as revenue
        FROM hub_deals d
        JOIN hub_contact_deal_associations a ON d.hubspot_deal_id = a.deal_hubspot_id
        JOIN hub_contacts c ON a.contact_hubspot_id = c.hubspot_id
        WHERE ${buildGoogleAdsAttributionQuery().replace(/hs_analytics_source/g, 'c.hs_analytics_source')
                                              .replace(/gclid/g, 'c.gclid')
                                              .replace(/hs_object_source_label/g, 'c.hs_object_source_label')
                                              .replace(/hs_analytics_first_touch_converting_campaign/g, 'c.hs_analytics_first_touch_converting_campaign')
                                              .replace(/hs_analytics_last_touch_converting_campaign/g, 'c.hs_analytics_last_touch_converting_campaign')}
          AND ${dateCondition}
        GROUP BY CASE 
          WHEN COALESCE(c.nationality, c.country, c.territory, c.ip_country, '') IN (${unsupportedTerritories.map(() => '?').join(',') || 'NULL'})
          THEN 'Unsupported Territory'
          ELSE COALESCE(c.nationality, c.country, c.territory, c.ip_country, 'Unknown')
        END
      `, [
        ...unsupportedTerritories, // For territory classification
        startDateStr, 
        endDateStr,
        ...unsupportedTerritories  // For territory classification (repeated)
      ]);
      
      // Combine contact and deal data
      const territoryMap = new Map();
      
      // Add contact data
      territoryContactsResult.forEach(t => {
        const rawTerritories = t.raw_territories ? 
          t.raw_territories.split('|').filter(territory => territory && territory.trim() !== '') : 
          [];
          
        territoryMap.set(t.territory, {
          name: t.territory,
          contacts: parseInt(t.contacts) || 0,
          deals_created: parseInt(t.deals_created) || 0,
          raw_territories: rawTerritories,
          deals: 0,
          won_deals: 0,
          lost_deals: 0,
          revenue: 0,
          // Debug info
          debug: {
            has_nationality: parseInt(t.has_nationality) || 0,
            has_country: parseInt(t.has_country) || 0,
            has_territory: parseInt(t.has_territory) || 0,
            has_ip_country: parseInt(t.has_ip_country) || 0
          }
        });
      });
      
      // Add deal data
      territoryDealsResult.forEach(t => {
        const territory = territoryMap.get(t.territory);
        if (territory) {
          territory.deals = parseInt(t.deals) || 0;
          territory.won_deals = parseInt(t.won_deals) || 0;
          territory.lost_deals = parseInt(t.lost_deals) || 0;
          territory.revenue = parseFloat(t.revenue) || 0;
        }
      });
      
      // Convert to array and calculate rates
      const territories = Array.from(territoryMap.values()).map((t, index) => {
        const isUnsupported = t.name === 'Unsupported Territory';
        const mqlToSqlRate = t.contacts > 0 ? ((t.deals_created / t.contacts) * 100).toFixed(1) : 0;
        const conversionRate = t.deals > 0 ? ((t.won_deals / t.deals) * 100).toFixed(1) : 0;
        
        return {
          ...t,
          mql_to_sql_rate: parseFloat(mqlToSqlRate),
          conversion_rate: parseFloat(conversionRate),
          isUnsupported: isUnsupported,
          burnRateFlag: isUnsupported && t.contacts > 0,
          color: isUnsupported ? '#EF4444' : getColorByIndex(index)
        };
      });
      
      // Calculate burn rate summary
      const unsupportedTerritory = territories.find(t => t.isUnsupported);
      const totalContacts = territories.reduce((sum, t) => sum + t.contacts, 0);
      const unsupportedContacts = unsupportedTerritory ? unsupportedTerritory.contacts : 0;
      
      console.log(`✅ Territory analysis: ${territories.length} territories, ${unsupportedContacts}/${totalContacts} unsupported`);
      
      // Debug unsupported territory
      if (unsupportedTerritory) {
        console.log(`🔥 Unsupported territory details:`, {
          contacts: unsupportedTerritory.contacts,
          raw_territories: unsupportedTerritory.raw_territories,
          debug: unsupportedTerritory.debug
        });
      } else {
        console.log(`⚠️ No unsupported territory found. Territory breakdown:`, 
          territories.map(t => ({ name: t.name, contacts: t.contacts }))
        );
      }
      
      return {
        success: true,
        territories: territories,
        burnRateSummary: {
          unsupportedContacts: unsupportedContacts,
          totalContacts: totalContacts,
          burnRatePercentage: totalContacts > 0 ? ((unsupportedContacts / totalContacts) * 100).toFixed(1) : 0
        },
        analysis_mode: analysisMode,
        debug: {
          unsupported_territories_loaded: unsupportedTerritories.length,
          territory_groups_found: territories.length,
          has_unsupported_group: !!unsupportedTerritory
        },
        timestamp: new Date().toISOString()
      };
      
    } finally {
      await connection.end();
    }
    
  } catch (error) {
    console.error('❌ Territory analysis failed:', error.message);
    return {
      success: false,
      error: error.message,
      timestamp: new Date().toISOString()
    };
  }
}

/**
 * Get trend data for dashboard charts
 */
async function getTrendData(getDbConnection, days = 30, analysisMode = 'pipeline') {
  try {
    const connection = await getDbConnection();
    
    try {
      console.log(`📈 Getting trend data for ${days} days (${analysisMode} mode)...`);
      
      // Generate date series for the last N days
      const dateCondition = analysisMode === 'revenue' 
        ? 'd.hs_closed_won_date'
        : 'd.createdate';
        
      const [trendResult] = await connection.execute(`
        SELECT 
          DATE(${dateCondition}) as date,
          COUNT(*) as deals,
          SUM(CAST(COALESCE(d.amount, '0') as DECIMAL(15,2))) as revenue,
          COUNT(CASE WHEN d.dealstage = 'closedwon' OR d.hs_is_closed_won = 'true' THEN 1 END) as won_deals
        FROM hub_deals d
        JOIN hub_contact_deal_associations a ON d.hubspot_deal_id = a.deal_hubspot_id
        JOIN hub_contacts c ON a.contact_hubspot_id = c.hubspot_id
        WHERE ${buildGoogleAdsAttributionQuery().replace(/hs_analytics_source/g, 'c.hs_analytics_source')
                                              .replace(/gclid/g, 'c.gclid')
                                              .replace(/hs_object_source_label/g, 'c.hs_object_source_label')
                                              .replace(/hs_analytics_first_touch_converting_campaign/g, 'c.hs_analytics_first_touch_converting_campaign')
                                              .replace(/hs_analytics_last_touch_converting_campaign/g, 'c.hs_analytics_last_touch_converting_campaign')}
          AND ${dateCondition} >= DATE_SUB(CURDATE(), INTERVAL ? DAY)
        GROUP BY DATE(${dateCondition})
        ORDER BY date DESC
        LIMIT ?
      `, [days, days]);
      
      const trends = trendResult.map(t => ({
        date: t.date,
        deals: parseInt(t.deals) || 0,
        revenue: parseFloat(t.revenue) || 0,
        won_deals: parseInt(t.won_deals) || 0
      }));
      
      return {
        success: true,
        trends: trends,
        analysis_mode: analysisMode,
        timestamp: new Date().toISOString()
      };
      
    } finally {
      await connection.end();
    }
    
  } catch (error) {
    console.error('❌ Trend data failed:', error.message);
    return {
      success: false,
      error: error.message,
      timestamp: new Date().toISOString()
    };
  }
}

module.exports = {
  getDashboardSummary,
  getCampaignPerformance,
  getTerritoryAnalysis,
  getTrendData,
  getMQLValidationMetrics,
  testGoogleAdsAttribution, // NEW - Moved from index
  loadCountryClassifications,
  buildGoogleAdsAttributionQuery
};