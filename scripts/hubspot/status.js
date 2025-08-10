/**
 * HubSpot Status Module - Clean version using Data Access Layer
 * No raw SQL queries - all database operations go through model classes
 */

// Import our data models
const hubLead = require('../models/hub-lead');
const hubPipeline = require('../models/hub-pipeline');
const hubCountry = require('../models/hub-country');
const hubDeal = require('../models/hub-deal');

/**
 * Get comprehensive HubSpot sync status
 * @param {Function} getDbConnection - Database connection factory from index.js
 * @returns {Object} Status information
 */
async function getHubSpotStatus(getDbConnection) {
  try {
    // Initialize our data models
    const leadModel = new LeadModel(getDbConnection);
    const pipelineModel = new PipelineModel(getDbConnection);
    const countryModel = new CountryModel(getDbConnection);
    const dealModel = new DealModel(getDbConnection);
    
    // Get all status data using models (no raw SQL)
    const [
      pipelineStages,
      territoryDistribution,
      recentActivity,
      territoryBurn,
      topCountries,
      databaseStats
    ] = await Promise.all([
      leadModel.getLeadsByPipelineStage(),
      leadModel.getTerritoryDistribution(),
      leadModel.getRecentActivity(24), // Last 24 hours
      leadModel.getTerritoryBurnRate(30), // Last 30 days
      leadModel.getTopCountries(30, 10), // Last 30 days, top 10
      dealModel.getDbStats()
    ]);
    
    return {
      service: 'HubSpot',
      status: 'active',
      pipeline_stages: pipelineStages,
      territory_distribution: territoryDistribution,
      recent_activity: recentActivity,
      territory_burn: territoryBurn,
      top_countries: topCountries,
      database_stats: databaseStats,
      timestamp: new Date().toISOString()
    };
    
  } catch (error) {
    console.error('❌ HubSpot status failed:', error.message);
    throw error;
  }
}

/**
 * Get HubSpot sync health metrics
 * @param {Function} getDbConnection - Database connection factory from index.js
 * @returns {Object} Health metrics
 */
async function getSyncHealth(getDbConnection) {
  try {
    // Initialize our data models
    const leadModel = new LeadModel(getDbConnection);
    
    // Get health metrics using models
    const [
      lastSync,
      dataQuality
    ] = await Promise.all([
      leadModel.getSyncHealth(),
      leadModel.getDataQuality()
    ]);
    
    return {
      last_sync: lastSync,
      data_quality: dataQuality,
      sync_status: lastSync.leads_last_hour > 0 ? 'active' : 'idle',
      timestamp: new Date().toISOString()
    };
    
  } catch (error) {
    console.error('❌ Sync health check failed:', error.message);
    throw error;
  }
}

/**
 * Get pipeline conversion metrics
 * @param {Function} getDbConnection - Database connection factory from index.js
 * @param {number} days - Number of days to analyze
 * @returns {Object} Conversion metrics
 */
async function getPipelineConversions(getDbConnection, days = 30) {
  try {
    // Initialize our data models
    const leadModel = new LeadModel(getDbConnection);
    
    // Get conversion data using model
    const conversions = await leadModel.getPipelineConversions(days);
    
    return {
      timeframe: `${days} days`,
      conversions: conversions,
      timestamp: new Date().toISOString()
    };
    
  } catch (error) {
    console.error('❌ Pipeline conversions failed:', error.message);
    throw error;
  }
}

/**
 * Get HubSpot integration summary
 * @param {Function} getDbConnection - Database connection factory from index.js
 * @returns {Object} Integration summary
 */
async function getIntegrationSummary(getDbConnection) {
  try {
    // Initialize our data models
    const leadModel = new LeadModel(getDbConnection);
    const pipelineModel = new PipelineModel(getDbConnection);
    const countryModel = new CountryModel(getDbConnection);
    
    // Get summary data using models
    const [
      syncOverview,
      territoryHealth,
      pipelineHealth
    ] = await Promise.all([
      leadModel.getOverviewStats(),
      countryModel.getTerritoryHealth(),
      pipelineModel.getPipelineHealth(30) // Last 30 days
    ]);
    
    return {
      sync_overview: syncOverview,
      territory_health: territoryHealth,
      pipeline_health: pipelineHealth,
      status: 'healthy',
      timestamp: new Date().toISOString()
    };
    
  } catch (error) {
    console.error('❌ Integration summary failed:', error.message);
    throw error;
  }
}

/**
 * Get territory analysis report
 * @param {Function} getDbConnection - Database connection factory from index.js
 * @param {number} days - Number of days to analyze
 * @returns {Object} Territory analysis
 */
async function getTerritoryAnalysis(getDbConnection, days = 30) {
  try {
    // Initialize our data models
    const countryModel = new CountryModel(getDbConnection);
    const leadModel = new LeadModel(getDbConnection);
    
    // Get territory analysis using models
    const [
      countryPerformance,
      territoryHealth,
      burnRate
    ] = await Promise.all([
      countryModel.getCountryPerformance(days),
      countryModel.getTerritoryHealth(),
      leadModel.getTerritoryBurnRate(days)
    ]);
    
    return {
      timeframe: `${days} days`,
      country_performance: countryPerformance,
      territory_health: territoryHealth,
      burn_rate: burnRate,
      recommendations: generateTerritoryRecommendations(burnRate, countryPerformance),
      timestamp: new Date().toISOString()
    };
    
  } catch (error) {
    console.error('❌ Territory analysis failed:', error.message);
    throw error;
  }
}

/**
 * Generate territory recommendations based on data
 */
function generateTerritoryRecommendations(burnRate, countryPerformance) {
  const recommendations = [];
  
  // High burn rate warning
  if (burnRate && burnRate.burn_rate > 20) {
    recommendations.push({
      type: 'warning',
      title: 'High Territory Burn Rate',
      message: `${burnRate.burn_rate}% of leads are from red countries. Consider improving targeting.`,
      action: 'Review ad targeting to focus on green/yellow countries'
    });
  }
  
  // Low performing countries
  const lowPerformers = countryPerformance.filter(country => 
    country.total_leads > 10 && country.responsive_rate < 5
  );
  
  if (lowPerformers.length > 0) {
    recommendations.push({
      type: 'optimization',
      title: 'Low Performing Countries',
      message: `${lowPerformers.length} countries have >10 leads but <5% response rate`,
      action: 'Consider moving these countries to red status',
      countries: lowPerformers.map(c => c.country_name)
    });
  }
  
  // High performers
  const highPerformers = countryPerformance.filter(country => 
    country.total_leads > 5 && country.responsive_rate > 15
  );
  
  if (highPerformers.length > 0) {
    recommendations.push({
      type: 'opportunity',
      title: 'High Performing Countries',
      message: `${highPerformers.length} countries showing >15% response rate`,
      action: 'Consider increasing ad spend in these countries',
      countries: highPerformers.map(c => c.country_name)
    });
  }
  
  return recommendations;
}

/**
 * Get dashboard summary for quick overview
 * @param {Function} getDbConnection - Database connection factory from index.js
 * @returns {Object} Dashboard summary
 */
async function getDashboardSummary(getDbConnection) {
  try {
    // Initialize our data models
    const leadModel = new LeadModel(getDbConnection);
    const dealModel = new DealModel(getDbConnection);
    
    // Get key metrics using models
    const [
      recentActivity,
      burnRate,
      dealStats,
      dataQuality
    ] = await Promise.all([
      leadModel.getRecentActivity(24), // Today
      leadModel.getTerritoryBurnRate(7), // This week
      dealModel.getDealStats(7), // This week
      leadModel.getDataQuality()
    ]);
    
    // Calculate health score
    const healthFactors = [
      burnRate.burn_rate < 20 ? 25 : Math.max(0, 25 - burnRate.burn_rate), // Burn rate factor
      dataQuality.quality_score * 0.25, // Data quality factor (25% weight)
      Math.min(25, recentActivity.leads_today * 2), // Activity factor
      Math.min(25, dealStats.won_deals * 5) // Success factor
    ];
    
    const healthScore = Math.round(healthFactors.reduce((a, b) => a + b, 0));
    
    return {
      health_score: healthScore,
      status: healthScore > 75 ? 'excellent' : healthScore > 50 ? 'good' : 'needs_attention',
      today: {
        leads: recentActivity.leads_today,
        responsive: recentActivity.responsive_today,
        won: recentActivity.won_today
      },
      this_week: {
        burn_rate: burnRate.burn_rate,
        deals: dealStats.recent_deals,
        deal_value: dealStats.recent_value
      },
      data_quality: {
        score: dataQuality.quality_score,
        total_records: dataQuality.total_leads
      },
      timestamp: new Date().toISOString()
    };
    
  } catch (error) {
    console.error('❌ Dashboard summary failed:', error.message);
    throw error;
  }
}

module.exports = {
  getHubSpotStatus,
  getSyncHealth,
  getPipelineConversions,
  getIntegrationSummary,
  getTerritoryAnalysis,
  getDashboardSummary
};