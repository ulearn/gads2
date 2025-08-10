/**
 * HubSpot Dashboard Data API
 * /scripts/analytics/hubspot-data.js
 * Pulls real data from synced MySQL HubSpot tables
 * Uses country reference file for territory classifications
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
 * Get dashboard summary metrics from HubSpot data
 */
async function getDashboardSummary(getDbConnection, days = 30) {
  try {
    const connection = await getDbConnection();
    
    try {
      const endDate = new Date();
      const startDate = new Date();
      startDate.setDate(startDate.getDate() - days);
      
      // Convert to MySQL datetime format
      const startDateStr = startDate.toISOString().slice(0, 19).replace('T', ' ');
      const endDateStr = endDate.toISOString().slice(0, 19).replace('T', ' ');
      
      console.log(`📊 Getting dashboard summary for ${days} days...`);
      
      // Get total contacts from Google Ads (PAID_SEARCH)
      const [contactsResult] = await connection.execute(`
        SELECT 
          COUNT(*) as total_contacts,
          COUNT(CASE WHEN lifecyclestage IN ('customer', 'opportunity') THEN 1 END) as converted_contacts,
          COUNT(CASE WHEN num_associated_deals > 0 THEN 1 END) as contacts_with_deals
        FROM hub_contacts 
        WHERE hs_analytics_source = 'PAID_SEARCH'
          AND createdate >= ? 
          AND createdate <= ?
      `, [startDateStr, endDateStr]);
      
      // Get deal metrics - simplified query
      const [dealsResult] = await connection.execute(`
        SELECT 
          COUNT(*) as total_deals,
          COALESCE(SUM(CAST(COALESCE(hs_analytics_revenue, '0') as DECIMAL(15,2))), 0) as total_deal_value,
          COALESCE(AVG(CAST(COALESCE(hs_analytics_revenue, '0') as DECIMAL(15,2))), 0) as avg_deal_value
        FROM hub_contacts
        WHERE hs_analytics_source = 'PAID_SEARCH'
          AND createdate >= ?
          AND createdate <= ?
          AND CAST(COALESCE(num_associated_deals, '0') as UNSIGNED) > 0
      `, [startDateStr, endDateStr]);
      
      // Get top campaigns (hs_analytics_source_data_1)
      const [campaignsResult] = await connection.execute(`
        SELECT 
          hs_analytics_source_data_1 as campaign_name,
          COUNT(*) as contacts,
          COUNT(CASE WHEN CAST(COALESCE(num_associated_deals, '0') as UNSIGNED) > 0 THEN 1 END) as deals,
          COALESCE(SUM(CAST(COALESCE(hs_analytics_revenue, '0') as DECIMAL(15,2))), 0) as revenue
        FROM hub_contacts 
        WHERE hs_analytics_source = 'PAID_SEARCH'
          AND createdate >= ? 
          AND createdate <= ?
          AND hs_analytics_source_data_1 IS NOT NULL
          AND hs_analytics_source_data_1 != ''
        GROUP BY hs_analytics_source_data_1
        ORDER BY contacts DESC
        LIMIT 10
      `, [startDateStr, endDateStr]);
      
      // Get territory data (using actual column names from your contact)
      const [territoriesResult] = await connection.execute(`
        SELECT 
          COALESCE(nationality, territory, 'Unknown') as territory,
          COUNT(*) as contacts,
          COUNT(CASE WHEN CAST(COALESCE(num_associated_deals, '0') as UNSIGNED) > 0 THEN 1 END) as deals,
          COALESCE(SUM(CAST(COALESCE(hs_analytics_revenue, '0') as DECIMAL(15,2))), 0) as revenue
        FROM hub_contacts 
        WHERE hs_analytics_source = 'PAID_SEARCH'
          AND createdate >= ? 
          AND createdate <= ?
        GROUP BY COALESCE(nationality, territory, 'Unknown')
        ORDER BY contacts DESC
        LIMIT 10
      `, [startDateStr, endDateStr]);
      
      const contacts = contactsResult[0];
      const deals = dealsResult[0];
      const campaigns = campaignsResult;
      const territories = territoriesResult;
      
      // Calculate metrics
      const conversionRate = contacts.total_contacts > 0 ? 
        ((contacts.contacts_with_deals / contacts.total_contacts) * 100).toFixed(1) : 0;
      
      console.log(`✅ Dashboard summary: ${contacts.total_contacts} contacts, ${deals.total_deals} deals`);
      
      return {
        success: true,
        period: `${days} days`,
        summary: {
          totalContacts: parseInt(contacts.total_contacts) || 0,
          totalDeals: parseInt(deals.total_deals) || 0,
          totalRevenue: parseFloat(deals.total_deal_value) || 0,
          avgDealValue: parseFloat(deals.avg_deal_value) || 0,
          conversionRate: parseFloat(conversionRate),
          contactsWithDeals: parseInt(contacts.contacts_with_deals) || 0,
          // Placeholders for Google Ads data (to be added later)
          totalSpend: 0,
          roas: 0,
          costPerContact: 0
        },
        campaigns: campaigns.map(c => ({
          name: c.campaign_name || 'Unknown Campaign',
          contacts: parseInt(c.contacts) || 0,
          deals: parseInt(c.deals) || 0,
          revenue: parseFloat(c.revenue) || 0,
          // Placeholders for spend/roas (from Google Ads)
          spend: 0,
          roas: 0
        })),
        territories: territories.map((t, index) => ({
          name: t.territory || 'Unknown',
          contacts: parseInt(t.contacts) || 0,
          deals: parseInt(t.deals) || 0,
          revenue: parseFloat(t.revenue) || 0,
          // Assign colors for the chart
          color: getColorByIndex(index)
        })),
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
 * Get trend data for charts (weekly/daily breakdown)
 */
async function getTrendData(getDbConnection, days = 30) {
  try {
    const connection = await getDbConnection();
    
    try {
      const endDate = new Date();
      const startDate = new Date();
      startDate.setDate(startDate.getDate() - days);
      
      const startDateStr = startDate.toISOString().slice(0, 19).replace('T', ' ');
      const endDateStr = endDate.toISOString().slice(0, 19).replace('T', ' ');
      
      console.log(`📈 Getting trend data for ${days} days...`);
      
      // Get weekly trend data
      const [trendsResult] = await connection.execute(`
        SELECT 
          DATE(createdate) as date,
          COUNT(*) as contacts,
          COUNT(CASE WHEN CAST(COALESCE(num_associated_deals, '0') as UNSIGNED) > 0 THEN 1 END) as deals,
          COALESCE(SUM(CAST(COALESCE(hs_analytics_revenue, '0') as DECIMAL(15,2))), 0) as revenue
        FROM hub_contacts 
        WHERE hs_analytics_source = 'PAID_SEARCH'
          AND createdate >= ? 
          AND createdate <= ?
        GROUP BY DATE(createdate)
        ORDER BY date ASC
      `, [startDateStr, endDateStr]);
      
      console.log(`✅ Trend data: ${trendsResult.length} data points`);
      
      return {
        success: true,
        trends: trendsResult.map(t => ({
          date: t.date,
          contacts: parseInt(t.contacts) || 0,
          deals: parseInt(t.deals) || 0,
          revenue: parseFloat(t.revenue) || 0,
          // Placeholder for spend (from Google Ads)
          spend: 0
        })),
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

/**
 * Get detailed campaign performance
 */
async function getCampaignPerformance(getDbConnection, days = 30) {
  try {
    const connection = await getDbConnection();
    
    try {
      const endDate = new Date();
      const startDate = new Date();
      startDate.setDate(startDate.getDate() - days);
      
      const startDateStr = startDate.toISOString().slice(0, 19).replace('T', ' ');
      const endDateStr = endDate.toISOString().slice(0, 19).replace('T', ' ');
      
      console.log(`🎯 Getting campaign performance for ${days} days...`);
      
      // Get detailed campaign metrics
      const [campaignsResult] = await connection.execute(`
        SELECT 
          hs_analytics_source_data_1 as campaign_name,
          hs_analytics_source_data_2 as keywords,
          COUNT(*) as contacts,
          COUNT(CASE WHEN CAST(COALESCE(num_associated_deals, '0') as UNSIGNED) > 0 THEN 1 END) as deals,
          COALESCE(SUM(CAST(COALESCE(hs_analytics_revenue, '0') as DECIMAL(15,2))), 0) as revenue,
          COALESCE(AVG(CAST(COALESCE(hs_analytics_num_page_views, '0') as UNSIGNED)), 0) as avg_page_views,
          COALESCE(AVG(CAST(COALESCE(hs_analytics_num_visits, '0') as UNSIGNED)), 0) as avg_visits,
          COUNT(CASE WHEN lifecyclestage IN ('customer', 'opportunity') THEN 1 END) as qualified_leads
        FROM hub_contacts 
        WHERE hs_analytics_source = 'PAID_SEARCH'
          AND createdate >= ? 
          AND createdate <= ?
          AND hs_analytics_source_data_1 IS NOT NULL
          AND hs_analytics_source_data_1 != ''
        GROUP BY hs_analytics_source_data_1, hs_analytics_source_data_2
        ORDER BY contacts DESC
        LIMIT 20
      `, [startDateStr, endDateStr]);
      
      console.log(`✅ Campaign performance: ${campaignsResult.length} campaigns`);
      
      return {
        success: true,
        campaigns: campaignsResult.map(c => ({
          name: c.campaign_name || 'Unknown Campaign',
          keywords: c.keywords || '',
          contacts: parseInt(c.contacts) || 0,
          deals: parseInt(c.deals) || 0,
          revenue: parseFloat(c.revenue) || 0,
          qualifiedLeads: parseInt(c.qualified_leads) || 0,
          avgPageViews: parseFloat(c.avg_page_views) || 0,
          avgVisits: parseFloat(c.avg_visits) || 0,
          conversionRate: c.contacts > 0 ? ((c.deals / c.contacts) * 100).toFixed(1) : 0,
          // Placeholders for Google Ads data
          spend: 0,
          clicks: 0,
          impressions: 0,
          cpc: 0,
          roas: 0
        })),
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
 * Get territory analysis using country reference file
 */
async function getTerritoryAnalysis(getDbConnection, days = 30) {
  try {
    const connection = await getDbConnection();
    
    try {
      const endDate = new Date();
      const startDate = new Date();
      startDate.setDate(startDate.getDate() - days);
      
      const startDateStr = startDate.toISOString().slice(0, 19).replace('T', ' ');
      const endDateStr = endDate.toISOString().slice(0, 19).replace('T', ' ');
      
      console.log(`🌍 Getting territory analysis for ${days} days...`);
      
      // Load unsupported territories from reference file
      const unsupportedTerritories = loadCountryClassifications();
      
      if (unsupportedTerritories.length === 0) {
        console.log('⚠️ No unsupported territories loaded - will show all territories separately');
      }
      
      // Get all territories first
      const [allTerritoriesResult] = await connection.execute(`
        SELECT 
          COALESCE(nationality, territory, 'Unknown') as territory,
          COUNT(*) as contacts,
          COUNT(CASE WHEN CAST(COALESCE(num_associated_deals, '0') as UNSIGNED) > 0 THEN 1 END) as deals,
          COUNT(CASE WHEN lifecyclestage = 'lead' THEN 1 END) as leads,
          COUNT(CASE WHEN lifecyclestage = 'marketingqualifiedlead' THEN 1 END) as mql,
          COUNT(CASE WHEN lifecyclestage = 'salesqualifiedlead' THEN 1 END) as sqlLeads,
          COUNT(CASE WHEN lifecyclestage = 'opportunity' THEN 1 END) as opportunities,
          COUNT(CASE WHEN lifecyclestage = 'customer' THEN 1 END) as customers,
          COALESCE(SUM(CAST(COALESCE(hs_analytics_revenue, '0') as DECIMAL(15,2))), 0) as revenue,
          COALESCE(AVG(CAST(COALESCE(hs_analytics_num_page_views, '0') as UNSIGNED)), 0) as avg_page_views,
          COALESCE(AVG(CAST(COALESCE(hs_analytics_num_visits, '0') as UNSIGNED)), 0) as avg_visits
        FROM hub_contacts 
        WHERE hs_analytics_source = 'PAID_SEARCH'
          AND createdate >= ? 
          AND createdate <= ?
        GROUP BY COALESCE(nationality, territory, 'Unknown')
        HAVING contacts > 0
        ORDER BY contacts DESC
      `, [startDateStr, endDateStr]);
      
      console.log(`✅ Territory analysis: ${allTerritoriesResult.length} territories found`);
      
      // Process territories and group unsupported ones
      const supportedTerritories = [];
      const unsupportedData = {
        contacts: 0,
        deals: 0,
        revenue: 0,
        leads: 0,
        mql: 0,
        sqlLeads: 0,
        opportunities: 0,
        customers: 0,
        avg_page_views: 0,
        avg_visits: 0,
        rawTerritories: []
      };
      
      for (const territory of allTerritoriesResult) {
        const territoryName = territory.territory;
        const isUnsupported = unsupportedTerritories.includes(territoryName);
        
        if (isUnsupported) {
          // Aggregate unsupported territory data
          unsupportedData.contacts += parseInt(territory.contacts) || 0;
          unsupportedData.deals += parseInt(territory.deals) || 0;
          unsupportedData.revenue += parseFloat(territory.revenue) || 0;
          unsupportedData.leads += parseInt(territory.leads) || 0;
          unsupportedData.mql += parseInt(territory.mql) || 0;
          unsupportedData.sqlLeads += parseInt(territory.sqlLeads) || 0;
          unsupportedData.opportunities += parseInt(territory.opportunities) || 0;
          unsupportedData.customers += parseInt(territory.customers) || 0;
          unsupportedData.rawTerritories.push(territoryName);
        } else {
          // Keep supported territories separate
          supportedTerritories.push(territory);
        }
      }
      
      // Build final territories array
      const territories = [];
      
      // Add unsupported territory group first (if any)
      if (unsupportedData.contacts > 0) {
        const conversionRate = unsupportedData.contacts > 0 ? 
          ((unsupportedData.deals / unsupportedData.contacts) * 100).toFixed(1) : 0;
          
        territories.push({
          name: 'Unsupported Territory',
          rawTerritories: unsupportedData.rawTerritories,
          contacts: unsupportedData.contacts,
          deals: unsupportedData.deals,
          revenue: unsupportedData.revenue,
          pipeline: {
            leads: unsupportedData.leads,
            mql: unsupportedData.mql,
            sql: unsupportedData.sqlLeads,
            opportunities: unsupportedData.opportunities,
            customers: unsupportedData.customers
          },
          avgPageViews: 0,
          avgVisits: 0,
          conversionRate: parseFloat(conversionRate),
          isUnsupported: true,
          burnRateFlag: true,
          color: '#EF4444' // Red for burn rate
        });
      }
      
      // Add supported territories
      supportedTerritories.forEach((t, index) => {
        const conversionRate = t.contacts > 0 ? ((t.deals / t.contacts) * 100).toFixed(1) : 0;
        
        territories.push({
          name: t.territory || 'Unknown',
          rawTerritories: [t.territory],
          contacts: parseInt(t.contacts) || 0,
          deals: parseInt(t.deals) || 0,
          revenue: parseFloat(t.revenue) || 0,
          pipeline: {
            leads: parseInt(t.leads) || 0,
            mql: parseInt(t.mql) || 0,
            sql: parseInt(t.sqlLeads) || 0,
            opportunities: parseInt(t.opportunities) || 0,
            customers: parseInt(t.customers) || 0
          },
          avgPageViews: parseFloat(t.avg_page_views) || 0,
          avgVisits: parseFloat(t.avg_visits) || 0,
          conversionRate: parseFloat(conversionRate),
          isUnsupported: false,
          burnRateFlag: false,
          color: getColorByIndex(index + 1) // +1 to account for unsupported territory
        });
      });
      
      // Calculate burn rate summary
      const totalContacts = territories.reduce((sum, t) => sum + t.contacts, 0);
      const unsupportedContacts = unsupportedData.contacts;
      
      return {
        success: true,
        territories: territories.slice(0, 20), // Limit to top 20
        burnRateSummary: {
          unsupportedContacts: unsupportedContacts,
          totalContacts: totalContacts,
          burnRatePercentage: totalContacts > 0 ? ((unsupportedContacts / totalContacts) * 100).toFixed(1) : 0,
          unsupportedTerritoriesCount: unsupportedTerritories.length
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
 * Helper function to assign colors for charts
 */
function getColorByIndex(index) {
  const colors = [
    '#3B82F6', // Blue
    '#10B981', // Green
    '#F59E0B', // Yellow
    '#EF4444', // Red
    '#8B5CF6', // Purple
    '#06B6D4', // Cyan
    '#F97316', // Orange
    '#84CC16', // Lime
    '#EC4899', // Pink
    '#6B7280'  // Gray
  ];
  return colors[index % colors.length];
}

module.exports = {
  getDashboardSummary,
  getTrendData,
  getCampaignPerformance,
  getTerritoryAnalysis
};