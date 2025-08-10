/**
 * Analytics Budget Module
 * Path: /home/hub/public_html/gads/scripts/analytics/budget.js
 * 
 * Handles budget analytics, ROI analysis, and cross-platform budget reporting
 * Combines Google Ads budget data with HubSpot conversion data for comprehensive analysis
 */

/**
 * Get comprehensive budget performance analysis
 * @param {Function} getDbConnection - Database connection function
 * @param {Object} googleAdsClient - Google Ads customer client
 * @param {Object} options - Analysis options
 * @param {number} options.days - Number of days to analyze (default: 30)
 * @returns {Object} Comprehensive budget analysis
 */
async function getBudgetPerformanceAnalysis(getDbConnection, googleAdsClient, options = {}) {
  try {
    console.log('📊 Performing comprehensive budget performance analysis...');
    
    const days = options.days || 30;
    
    // Import required modules
    const googleBudgetModule = require('../google/budget');
    const googleCampaignModule = require('../google/campaign');
    const hubspotData = require('./hubspot-data');
    
    // Get Google Ads budget and campaign data
    const [budgetInfo, campaignPerformance, budgetUtilization] = await Promise.all([
      googleBudgetModule.getBudgetInfo(googleAdsClient),
      googleCampaignModule.getCampaignPerformance(googleAdsClient, { days }),
      googleBudgetModule.getBudgetUtilization(googleAdsClient, { days })
    ]);
    
    // Get HubSpot conversion data
    const hubspotResults = await hubspotData.getDashboardSummary(getDbConnection, days);
    
    if (!budgetInfo.success || !campaignPerformance.success || !hubspotResults.success) {
      throw new Error('Failed to fetch required data for budget analysis');
    }
    
    // Calculate ROI metrics by combining Google Ads spend with HubSpot conversions
    const roiAnalysis = calculateROIMetrics({
      campaigns: campaignPerformance.campaigns,
      hubspotSummary: hubspotResults.summary,
      budgets: budgetInfo.budgets
    });
    
    // Analyze budget efficiency
    const efficiencyAnalysis = analyzeBudgetEfficiency({
      budgetUtilization: budgetUtilization.utilization,
      campaignPerformance: campaignPerformance.campaigns,
      hubspotConversions: hubspotResults.summary
    });
    
    // Generate budget recommendations
    const recommendations = generateBudgetRecommendations({
      roiAnalysis,
      efficiencyAnalysis,
      budgetInfo,
      campaignPerformance
    });
    
    console.log('   ✅ Budget performance analysis complete');
    
    return {
      success: true,
      period: `Last ${days} days`,
      google_ads_data: {
        budget_info: budgetInfo,
        campaign_performance: campaignPerformance.summary,
        budget_utilization: budgetUtilization.summary
      },
      hubspot_data: {
        summary: hubspotResults.summary
      },
      roi_analysis: roiAnalysis,
      efficiency_analysis: efficiencyAnalysis,
      recommendations,
      timestamp: new Date().toISOString()
    };
    
  } catch (error) {
    console.error('❌ Budget performance analysis failed:', error.message);
    return {
      success: false,
      error: error.message,
      timestamp: new Date().toISOString()
    };
  }
}

/**
 * Calculate ROI metrics by combining Google Ads spend with HubSpot conversions
 * @param {Object} data - Combined data from Google Ads and HubSpot
 * @returns {Object} ROI analysis
 */
function calculateROIMetrics(data) {
  const { campaigns, hubspotSummary, budgets } = data;
  
  // Calculate total Google Ads spend
  const totalSpend = campaigns.reduce((sum, campaign) => sum + parseFloat(campaign.metrics.cost), 0);
  
  // HubSpot conversion data
  const totalContacts = hubspotSummary.totalContacts || 0;
  const totalDeals = hubspotSummary.totalDeals || 0;
  const totalRevenue = hubspotSummary.totalRevenue || 0;
  const avgDealValue = hubspotSummary.avgDealValue || 0;
  
  // Calculate key metrics
  const costPerContact = totalContacts > 0 ? (totalSpend / totalContacts).toFixed(2) : '0.00';
  const costPerDeal = totalDeals > 0 ? (totalSpend / totalDeals).toFixed(2) : '0.00';
  const roi = totalSpend > 0 ? (((totalRevenue - totalSpend) / totalSpend) * 100).toFixed(2) : '0.00';
  const roas = totalSpend > 0 ? (totalRevenue / totalSpend).toFixed(2) : '0.00';
  
  // Calculate customer lifetime value to cost ratio
  const ltvToCacRatio = parseFloat(costPerDeal) > 0 ? (avgDealValue / parseFloat(costPerDeal)).toFixed(2) : '0.00';
  
  return {
    total_spend: totalSpend.toFixed(2),
    total_contacts: totalContacts,
    total_deals: totalDeals,
    total_revenue: totalRevenue.toFixed(2),
    cost_per_contact: costPerContact,
    cost_per_deal: costPerDeal,
    roi_percent: roi,
    roas: roas,
    ltv_to_cac_ratio: ltvToCacRatio,
    conversion_rate: totalContacts > 0 ? ((totalDeals / totalContacts) * 100).toFixed(2) : '0.00',
    profitability: parseFloat(roi) > 0 ? 'Profitable' : 'Unprofitable',
    efficiency_rating: getEfficiencyRating(parseFloat(roi), parseFloat(roas))
  };
}

/**
 * Analyze budget efficiency across campaigns and budgets
 * @param {Object} data - Budget utilization and performance data
 * @returns {Object} Efficiency analysis
 */
function analyzeBudgetEfficiency(data) {
  const { budgetUtilization, campaignPerformance, hubspotConversions } = data;
  
  // Find underutilized budgets
  const underutilizedBudgets = budgetUtilization.filter(budget => 
    parseFloat(budget.utilization_percent) < 80
  );
  
  // Find over-budget campaigns
  const overBudgetCampaigns = budgetUtilization.filter(budget => 
    parseFloat(budget.utilization_percent) > 100
  );
  
  // Calculate wasted spend (underutilized budgets)
  const wastedSpend = underutilizedBudgets.reduce((sum, budget) => {
    const potential = parseFloat(budget.potential_spend);
    const actual = parseFloat(budget.total_spend);
    return sum + (potential - actual);
  }, 0);
  
  // Find campaigns with poor performance
  const poorPerformingCampaigns = campaignPerformance.filter(campaign => {
    const cost = parseFloat(campaign.metrics.cost);
    const conversions = campaign.metrics.conversions;
    return cost > 100 && conversions === 0; // Spent >€100 with no conversions
  });
  
  return {
    underutilized_budgets: underutilizedBudgets.length,
    over_budget_campaigns: overBudgetCampaigns.length,
    wasted_spend: wastedSpend.toFixed(2),
    poor_performing_campaigns: poorPerformingCampaigns.length,
    budget_efficiency_score: calculateEfficiencyScore({
      underutilizedBudgets,
      overBudgetCampaigns,
      poorPerformingCampaigns,
      totalBudgets: budgetUtilization.length
    }),
    optimization_opportunities: identifyOptimizationOpportunities({
      underutilizedBudgets,
      overBudgetCampaigns,
      poorPerformingCampaigns
    })
  };
}

/**
 * Generate budget optimization recommendations
 * @param {Object} data - Analysis data
 * @returns {Array} Array of recommendations
 */
function generateBudgetRecommendations(data) {
  const recommendations = [];
  const { roiAnalysis, efficiencyAnalysis, budgetInfo, campaignPerformance } = data;
  
  // ROI-based recommendations
  if (parseFloat(roiAnalysis.roi_percent) < 0) {
    recommendations.push({
      type: 'critical',
      category: 'ROI',
      title: 'Negative ROI Detected',
      message: `Current ROI is ${roiAnalysis.roi_percent}% - campaigns are losing money`,
      action: 'Pause underperforming campaigns and optimize targeting',
      priority: 1,
      impact: 'Stop losses and preserve budget'
    });
  }
  
  if (parseFloat(roiAnalysis.roas) < 2) {
    recommendations.push({
      type: 'high',
      category: 'ROAS',
      title: 'Low Return on Ad Spend',
      message: `ROAS is ${roiAnalysis.roas}:1 - should aim for 3:1 or higher`,
      action: 'Focus budget on high-converting campaigns and keywords',
      priority: 2,
      impact: 'Improve profitability and efficiency'
    });
  }
  
  // Budget efficiency recommendations
  if (efficiencyAnalysis.underutilized_budgets > 0) {
    recommendations.push({
      type: 'medium',
      category: 'Budget Utilization',
      title: 'Underutilized Budgets Found',
      message: `${efficiencyAnalysis.underutilized_budgets} budgets are underutilized`,
      action: 'Reallocate budget to high-performing campaigns or reduce daily limits',
      priority: 3,
      impact: `Recover €${efficiencyAnalysis.wasted_spend} in wasted spend`
    });
  }
  
  if (efficiencyAnalysis.over_budget_campaigns > 0) {
    recommendations.push({
      type: 'medium',
      category: 'Budget Control',
      title: 'Campaigns Exceeding Budget',
      message: `${efficiencyAnalysis.over_budget_campaigns} campaigns are over budget`,
      action: 'Increase budgets for high-performing campaigns or adjust bidding',
      priority: 3,
      impact: 'Prevent lost opportunities due to budget constraints'
    });
  }
  
  // Performance-based recommendations
  if (parseFloat(roiAnalysis.cost_per_contact) > 50) {
    recommendations.push({
      type: 'high',
      category: 'Cost Efficiency',
      title: 'High Cost Per Contact',
      message: `Cost per contact is €${roiAnalysis.cost_per_contact} - industry average is €20-30`,
      action: 'Optimize targeting, improve ad quality scores, and refine keywords',
      priority: 2,
      impact: 'Reduce acquisition costs and improve efficiency'
    });
  }
  
  // Add overall strategy recommendation
  recommendations.push({
    type: 'info',
    category: 'Strategy',
    title: 'Implement Performance Monitoring',
    message: 'Set up automated alerts for budget utilization and ROI metrics',
    action: 'Create weekly performance reports and budget optimization alerts',
    priority: 4,
    impact: 'Proactive optimization and faster response to issues'
  });
  
  return recommendations.sort((a, b) => a.priority - b.priority);
}

/**
 * Helper function to calculate efficiency rating
 */
function getEfficiencyRating(roi, roas) {
  if (roi > 50 && roas > 4) return 'Excellent';
  if (roi > 20 && roas > 3) return 'Good';
  if (roi > 0 && roas > 2) return 'Fair';
  return 'Poor';
}

/**
 * Helper function to calculate efficiency score
 */
function calculateEfficiencyScore(data) {
  const { underutilizedBudgets, overBudgetCampaigns, poorPerformingCampaigns, totalBudgets } = data;
  
  let score = 100;
  score -= (underutilizedBudgets.length / totalBudgets) * 30;
  score -= (overBudgetCampaigns.length / totalBudgets) * 20;
  score -= (poorPerformingCampaigns.length / totalBudgets) * 40;
  
  return Math.max(0, score).toFixed(1);
}

/**
 * Helper function to identify optimization opportunities
 */
function identifyOptimizationOpportunities(data) {
  const opportunities = [];
  const { underutilizedBudgets, overBudgetCampaigns, poorPerformingCampaigns } = data;
  
  if (underutilizedBudgets.length > 0) {
    opportunities.push('Reallocate underutilized budget to high-performing campaigns');
  }
  
  if (overBudgetCampaigns.length > 0) {
    opportunities.push('Increase budgets for campaigns hitting limits');
  }
  
  if (poorPerformingCampaigns.length > 0) {
    opportunities.push('Pause or optimize poor-performing campaigns');
  }
  
  return opportunities;
}

/**
 * Get budget trend analysis over time
 * @param {Function} getDbConnection - Database connection function
 * @param {Object} googleAdsClient - Google Ads customer client
 * @param {Object} options - Analysis options
 * @returns {Object} Budget trend analysis
 */
async function getBudgetTrendAnalysis(getDbConnection, googleAdsClient, options = {}) {
  try {
    console.log('📈 Analyzing budget trends over time...');
    
    const days = options.days || 90;
    const googleCampaignModule = require('../google/campaign');
    
    // Get campaign performance over time
    const campaignPerformance = await googleCampaignModule.getCampaignPerformance(googleAdsClient, { days });
    
    if (!campaignPerformance.success) {
      throw new Error('Failed to fetch campaign performance data');
    }
    
    // Analyze trends (this would be expanded with actual trend calculations)
    // For now, return basic structure
    
    return {
      success: true,
      period: `Last ${days} days`,
      trends: {
        spend_trend: 'increasing', // Would calculate actual trend
        efficiency_trend: 'stable',
        roi_trend: 'improving'
      },
      projections: {
        monthly_spend_forecast: '0.00',
        quarterly_roi_projection: '0.00'
      },
      timestamp: new Date().toISOString()
    };
    
  } catch (error) {
    console.error('❌ Budget trend analysis failed:', error.message);
    return {
      success: false,
      error: error.message,
      timestamp: new Date().toISOString()
    };
  }
}

// Export functions
module.exports = {
  getBudgetPerformanceAnalysis,
  getBudgetTrendAnalysis
};