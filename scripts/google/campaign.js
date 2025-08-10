/**
 * Google Ads Campaign Data Module - Adaptive for All Campaign Types
 * Path: /home/hub/public_html/gads/scripts/google/campaign.js
 * 
 * Handles all Google Ads campaign types with adaptive queries:
 * - Performance Max: Basic data only (limited API access)
 * - Search: Full targeting, keywords, demographics
 * - Display: Targeting and audience data
 * - Video: YouTube targeting
 * - Shopping: Product targeting
 */

// Campaign type constants
const CAMPAIGN_TYPES = {
  PERFORMANCE_MAX: 10,
  SEARCH: 2,
  DISPLAY: 3,
  VIDEO: 6,
  SHOPPING: 5,
  APP: 12
};

/**
 * Get campaign performance data - adapts based on campaign type
 * @param {Object} customer - Authenticated Google Ads customer client from index.js
 * @param {Object} options - Query options
 * @param {number} options.days - Number of days back to query (default: 30)
 * @param {string} options.startDate - Start date in YYYY-MM-DD format
 * @param {string} options.endDate - End date in YYYY-MM-DD format
 * @returns {Object} Campaign performance data
 */
async function getCampaignPerformance(customer, options = {}) {
  try {
    console.log('🎯 Fetching Google Ads campaign performance (adaptive)...');
    
    if (!customer) {
      throw new Error('Google Ads customer client is required');
    }
    
    // Step 1: Get basic campaign info for all types
    const campaigns = await getBasicCampaignInfo(customer);
    
    if (!campaigns.success) {
      throw new Error(campaigns.error);
    }
    
    // Step 2: Enhance each campaign based on its type
    const enhancedCampaigns = await Promise.all(
      campaigns.campaigns.map(campaign => enhanceCampaignByType(customer, campaign, options))
    );
    
    // Step 3: Calculate summary
    const summary = calculateCampaignSummary(enhancedCampaigns);
    
    console.log(`   ✅ Adaptive campaign analysis complete: ${summary.total_campaigns} campaigns`);
    
    return {
      success: true,
      summary,
      campaigns: enhancedCampaigns,
      period: options.startDate && options.endDate ? 
        `${options.startDate} to ${options.endDate}` : 
        `Last ${options.days || 30} days`,
      timestamp: new Date().toISOString()
    };
    
  } catch (error) {
    console.error('❌ Adaptive campaign performance fetch failed:', error.message);
    return {
      success: false,
      error: error.message,
      timestamp: new Date().toISOString()
    };
  }
}

/**
 * Get basic campaign information for all campaign types
 * @param {Object} customer - Google Ads customer client
 * @returns {Object} Basic campaign data
 */
async function getBasicCampaignInfo(customer) {
  try {
    console.log('   📊 Fetching basic campaign information...');
    
    const query = `
      SELECT 
        campaign.id,
        campaign.name,
        campaign.status,
        campaign.advertising_channel_type,
        campaign.start_date,
        campaign.end_date,
        campaign.bidding_strategy_type,
        campaign.target_spend.target_spend_micros,
        campaign_budget.id,
        campaign_budget.name,
        campaign_budget.amount_micros,
        campaign_budget.delivery_method,
        campaign_budget.explicitly_shared
      FROM campaign 
      WHERE campaign.status != 'REMOVED'
      ORDER BY campaign.name
    `;
    
    const results = await customer.query(query);
    
    const campaigns = results.map(row => {
      const campaign = row.campaign;
      const budget = row.campaign_budget;
      
      return {
        id: campaign.id?.toString(),
        name: campaign.name,
        status: campaign.status,
        type: campaign.advertising_channel_type,
        type_name: getCampaignTypeName(campaign.advertising_channel_type),
        start_date: campaign.start_date,
        end_date: campaign.end_date,
        bidding_strategy: campaign.bidding_strategy_type,
        target_spend_micros: campaign.target_spend?.target_spend_micros || 0,
        budget: {
          id: budget?.id?.toString(),
          name: budget?.name,
          daily_amount_micros: budget?.amount_micros || 0,
          daily_amount: budget?.amount_micros ? 
            (budget.amount_micros / 1000000).toFixed(2) : '0.00',
          monthly_estimate: budget?.amount_micros ? 
            ((budget.amount_micros / 1000000) * 30.44).toFixed(2) : '0.00',
          delivery_method: budget?.delivery_method,
          shared: budget?.explicitly_shared || false
        },
        capabilities: getCampaignCapabilities(campaign.advertising_channel_type),
        metrics: {
          impressions: 0,
          clicks: 0,
          cost: '0.00',
          conversions: 0
        },
        targeting: null, // Will be populated by type-specific functions
        keywords: null,
        demographics: null,
        error_messages: []
      };
    });
    
    console.log(`   ✅ Found ${campaigns.length} campaigns`);
    
    return {
      success: true,
      campaigns
    };
    
  } catch (error) {
    console.error('   ❌ Basic campaign info fetch failed:', error.message);
    return {
      success: false,
      error: error.message
    };
  }
}

/**
 * Enhance campaign data based on its type
 * @param {Object} customer - Google Ads customer client  
 * @param {Object} campaign - Basic campaign data
 * @param {Object} options - Query options
 * @returns {Object} Enhanced campaign data
 */
async function enhanceCampaignByType(customer, campaign, options) {
  console.log(`   🔍 Enhancing ${campaign.type_name} campaign: ${campaign.name}`);
  
  try {
    // Get metrics for all campaign types (if possible)
    await addCampaignMetrics(customer, campaign, options);
    
    // Add type-specific data
    switch (campaign.type) {
      case CAMPAIGN_TYPES.PERFORMANCE_MAX:
        await enhancePerformanceMaxCampaign(customer, campaign, options);
        break;
        
      case CAMPAIGN_TYPES.SEARCH:
        await enhanceSearchCampaign(customer, campaign, options);
        break;
        
      case CAMPAIGN_TYPES.DISPLAY:
        await enhanceDisplayCampaign(customer, campaign, options);
        break;
        
      case CAMPAIGN_TYPES.VIDEO:
        await enhanceVideoCampaign(customer, campaign, options);
        break;
        
      case CAMPAIGN_TYPES.SHOPPING:
        await enhanceShoppingCampaign(customer, campaign, options);
        break;
        
      default:
        console.log(`   ⚠️ Unknown campaign type ${campaign.type} - using basic data only`);
        campaign.error_messages.push(`Unknown campaign type: ${campaign.type}`);
    }
    
    return campaign;
    
  } catch (error) {
    console.error(`   ❌ Failed to enhance ${campaign.type_name} campaign:`, error.message);
    campaign.error_messages.push(error.message);
    return campaign;
  }
}

/**
 * Add performance metrics to campaign (works for most types)
 */
async function addCampaignMetrics(customer, campaign, options) {
  try {
    const days = options.days || 7; // Use shorter period for reliability
    
    const metricsQuery = `
      SELECT 
        campaign.id,
        metrics.impressions,
        metrics.clicks,
        metrics.ctr,
        metrics.average_cpc,
        metrics.cost_micros,
        metrics.conversions,
        metrics.conversion_rate,
        metrics.view_through_conversions
      FROM campaign
      WHERE campaign.id = ${campaign.id}
        AND segments.date DURING LAST_${days}_DAYS
    `;
    
    const results = await customer.query(metricsQuery);
    
    // Aggregate metrics
    let totalMetrics = {
      impressions: 0,
      clicks: 0,
      cost_micros: 0,
      conversions: 0,
      view_through_conversions: 0
    };
    
    results.forEach(row => {
      totalMetrics.impressions += row.metrics?.impressions || 0;
      totalMetrics.clicks += row.metrics?.clicks || 0;
      totalMetrics.cost_micros += row.metrics?.cost_micros || 0;
      totalMetrics.conversions += row.metrics?.conversions || 0;
      totalMetrics.view_through_conversions += row.metrics?.view_through_conversions || 0;
    });
    
    // Calculate derived metrics
    campaign.metrics = {
      impressions: totalMetrics.impressions,
      clicks: totalMetrics.clicks,
      cost: (totalMetrics.cost_micros / 1000000).toFixed(2),
      conversions: totalMetrics.conversions,
      view_through_conversions: totalMetrics.view_through_conversions,
      ctr: totalMetrics.impressions > 0 ? 
        ((totalMetrics.clicks / totalMetrics.impressions) * 100).toFixed(2) : '0.00',
      cpc: totalMetrics.clicks > 0 ? 
        (totalMetrics.cost_micros / 1000000 / totalMetrics.clicks).toFixed(2) : '0.00',
      conversion_rate: totalMetrics.clicks > 0 ? 
        ((totalMetrics.conversions / totalMetrics.clicks) * 100).toFixed(2) : '0.00',
      cost_per_conversion: totalMetrics.conversions > 0 ? 
        (totalMetrics.cost_micros / 1000000 / totalMetrics.conversions).toFixed(2) : '0.00'
    };
    
  } catch (error) {
    console.log(`   ⚠️ Metrics unavailable for campaign ${campaign.name}: ${error.message}`);
    campaign.error_messages.push(`Metrics unavailable: ${error.message}`);
  }
}

/**
 * Enhance Performance Max campaign (limited data available)
 */
async function enhancePerformanceMaxCampaign(customer, campaign, options) {
  console.log(`   📱 Performance Max campaign: Getting available targeting data`);
  
  // Performance Max DOES expose targeting settings (just not detailed performance by target)
  campaign.targeting = await getCampaignTargeting(customer, campaign.id);
  campaign.demographics = await getCampaignDemographics(customer, campaign.id);
  
  // What Performance Max doesn't expose
  campaign.keywords = {
    note: "Performance Max uses automatic keyword discovery - specific keywords not available",
    available: false
  };
  
  // Try to get asset group info (Performance Max specific)
  try {
    const assetGroupQuery = `
      SELECT 
        asset_group.id,
        asset_group.name,
        asset_group.status
      FROM asset_group
      WHERE campaign.id = ${campaign.id}
    `;
    
    const assetGroups = await customer.query(assetGroupQuery);
    
    campaign.asset_groups = assetGroups.map(row => ({
      id: row.asset_group.id?.toString(),
      name: row.asset_group.name,
      status: row.asset_group.status
    }));
    
  } catch (error) {
    console.log(`   ⚠️ Asset groups unavailable: ${error.message}`);
    campaign.error_messages.push(`Asset groups unavailable: ${error.message}`);
  }
}

/**
 * Enhance Search campaign (full data available)
 */
async function enhanceSearchCampaign(customer, campaign, options) {
  console.log(`   🔍 Search campaign: Full data available`);
  
  // Get geographic targeting
  campaign.targeting = await getCampaignTargeting(customer, campaign.id);
  
  // Get keywords
  campaign.keywords = await getCampaignKeywords(customer, campaign.id, options);
  
  // Get demographics
  campaign.demographics = await getCampaignDemographics(customer, campaign.id);
}

/**
 * Enhance Display campaign
 */
async function enhanceDisplayCampaign(customer, campaign, options) {
  console.log(`   🖼️ Display campaign: Targeting and placement data`);
  
  campaign.targeting = await getCampaignTargeting(customer, campaign.id);
  campaign.demographics = await getCampaignDemographics(customer, campaign.id);
  
  // Display-specific: Get placement targeting
  try {
    const placementQuery = `
      SELECT 
        campaign_criterion.placement.url,
        campaign_criterion.negative
      FROM campaign_criterion
      WHERE campaign.id = ${campaign.id}
        AND campaign_criterion.type = 'PLACEMENT'
    `;
    
    const placements = await customer.query(placementQuery);
    campaign.placements = placements.map(row => ({
      url: row.campaign_criterion.placement?.url,
      negative: row.campaign_criterion.negative
    }));
    
  } catch (error) {
    console.log(`   ⚠️ Placements unavailable: ${error.message}`);
    campaign.error_messages.push(`Placements unavailable: ${error.message}`);
  }
}

/**
 * Enhance Video campaign (YouTube)  
 */
async function enhanceVideoCampaign(customer, campaign, options) {
  console.log(`   📹 Video campaign: YouTube targeting data`);
  
  campaign.targeting = await getCampaignTargeting(customer, campaign.id);
  campaign.demographics = await getCampaignDemographics(customer, campaign.id);
  
  // Video-specific targeting would go here
}

/**
 * Enhance Shopping campaign
 */
async function enhanceShoppingCampaign(customer, campaign, options) {
  console.log(`   🛍️ Shopping campaign: Product targeting data`);
  
  campaign.targeting = await getCampaignTargeting(customer, campaign.id);
  
  // Shopping-specific: Product groups, merchant center data
}

/**
 * Get campaign targeting data (geographic, etc.) - FIXED for Google Ads API
 * Works for Search, Display, Video - NOT Performance Max (but we can try)
 */
async function getCampaignTargeting(customer, campaignId) {
  try {
    console.log(`   📍 Getting targeting for campaign ${campaignId}...`);
    
    // Step 1: Get the location criteria (without joining geo_target_constant)
    const criteriaQuery = `
      SELECT 
        campaign_criterion.campaign,
        campaign_criterion.criterion_id,
        campaign_criterion.location.geo_target_constant,
        campaign_criterion.negative
      FROM campaign_criterion
      WHERE campaign.id = ${campaignId}
        AND campaign_criterion.type = 'LOCATION'
    `;
    
    console.log('   📊 Executing criteria query...');
    const criteriaResults = await customer.query(criteriaQuery);
    
    console.log(`   ✅ Found ${criteriaResults.length} location criteria`);
    
    // Step 2: Get location details for each geo target constant
    const locationDetails = new Map();
    
    if (criteriaResults.length > 0) {
      // Get unique geo target constants - THIS WAS MISSING!
      const geoTargetConstants = [...new Set(
        criteriaResults
          .map(row => row.campaign_criterion.location?.geo_target_constant)
          .filter(id => id)
      )];
      
      console.log(`   📍 Looking up details for ${geoTargetConstants.length} locations...`);
      
      // Query geo target constants separately
      if (geoTargetConstants.length > 0) {
        // Extract numeric IDs from "geoTargetConstants/2050" format
        const numericIds = geoTargetConstants.map(id => 
          id.replace('geoTargetConstants/', '')
        );
        
        const locationQuery = `
          SELECT 
            geo_target_constant.id,
            geo_target_constant.canonical_name,
            geo_target_constant.country_code,
            geo_target_constant.target_type,
            geo_target_constant.status
          FROM geo_target_constant
          WHERE geo_target_constant.id IN (${numericIds.join(',')})
        `;
        
        const locationResults = await customer.query(locationQuery);
        
        // Map location details using numeric ID as key
        locationResults.forEach(row => {
          const geoTarget = row.geo_target_constant;
          locationDetails.set(geoTarget.id?.toString(), {
            name: geoTarget.canonical_name,
            country_code: geoTarget.country_code,
            target_type: geoTarget.target_type,
            status: geoTarget.status
          });
        });
      }
    } // THIS CLOSING BRACE WAS MISSING!
    
    // Step 3: Combine criteria with location details
    const included = [];
    const excluded = [];
    
    criteriaResults.forEach(row => {
      const criterion = row.campaign_criterion;
      const geoTargetId = criterion.location?.geo_target_constant?.replace('geoTargetConstants/', '');
      const locationInfo = locationDetails.get(geoTargetId) || {};

      const location = {
        criterion_id: criterion.criterion_id?.toString(),
        geo_target_constant: geoTargetId,
        name: locationInfo.name || `Unknown Location (${geoTargetId})`,
        country_code: locationInfo.country_code,
        target_type: locationInfo.target_type,
        status: locationInfo.status
      };
      
      if (criterion.negative) {
        excluded.push(location);
      } else {
        included.push(location);
      }
    });
    
    console.log(`   ✅ Targeting analysis: ${included.length} included, ${excluded.length} excluded`);
    
    return {
      geographic: {
        included_locations: included,
        excluded_locations: excluded,
        available: true,
        total_criteria: criteriaResults.length
      }
    };
    
  } catch (error) {
    console.error(`   ❌ Targeting query failed: ${error.message}`);
    return {
      geographic: {
        error: error.message,
        available: false
      }
    };
  }
}

/**
 * Get campaign keywords (Search campaigns only)
 */
async function getCampaignKeywords(customer, campaignId, options) {
  try {
    const days = options.days || 30;
    
    const keywordQuery = `
      SELECT 
        ad_group_criterion.keyword.text,
        ad_group_criterion.keyword.match_type,
        metrics.impressions,
        metrics.clicks,
        metrics.cost_micros
      FROM keyword_view
      WHERE campaign.id = ${campaignId}
        AND segments.date DURING LAST_${days}_DAYS
        AND ad_group_criterion.status != 'REMOVED'
      ORDER BY metrics.cost_micros DESC
      LIMIT 50
    `;
    
    const results = await customer.query(keywordQuery);
    
    return {
      keywords: results.map(row => ({
        text: row.ad_group_criterion.keyword?.text,
        match_type: row.ad_group_criterion.keyword?.match_type,
        impressions: row.metrics?.impressions || 0,
        clicks: row.metrics?.clicks || 0,
        cost: row.metrics?.cost_micros ? (row.metrics.cost_micros / 1000000).toFixed(2) : '0.00'
      })),
      available: true
    };
    
  } catch (error) {
    return {
      error: error.message,
      available: false
    };
  }
}

/**
 * Get campaign demographics
 */
async function getCampaignDemographics(customer, campaignId) {
  try {
    const demoQuery = `
      SELECT 
        campaign_criterion.age_range.type,
        campaign_criterion.gender.type,
        campaign_criterion.negative
      FROM campaign_criterion
      WHERE campaign.id = ${campaignId}
        AND campaign_criterion.type IN ('AGE_RANGE', 'GENDER')
    `;
    
    const results = await customer.query(demoQuery);
    
    const ageRanges = [];
    const genders = [];
    
    results.forEach(row => {
      if (row.campaign_criterion.age_range) {
        ageRanges.push({
          type: row.campaign_criterion.age_range.type,
          negative: row.campaign_criterion.negative
        });
      }
      
      if (row.campaign_criterion.gender) {
        genders.push({
          type: row.campaign_criterion.gender.type,
          negative: row.campaign_criterion.negative
        });
      }
    });
    
    return {
      age_ranges: ageRanges,
      genders: genders,
      available: true
    };
    
  } catch (error) {
    return {
      error: error.message,
      available: false
    };
  }
}

/**
 * Helper functions
 */
function getCampaignTypeName(type) {
  const typeNames = {
    2: 'Search',
    3: 'Display', 
    5: 'Shopping',
    6: 'Video',
    10: 'Performance Max',
    12: 'App'
  };
  return typeNames[type] || `Unknown (${type})`;
}

function getCampaignCapabilities(type) {
  const capabilities = {
    2: { // Search
      keywords: true,
      geographic_targeting: true,
      demographic_targeting: true,
      detailed_metrics: true
    },
    3: { // Display
      keywords: false,
      geographic_targeting: true,
      demographic_targeting: true,
      placements: true,
      detailed_metrics: true
    },
    10: { // Performance Max
      keywords: false,
      geographic_targeting: false,
      demographic_targeting: false,
      detailed_metrics: false,
      asset_groups: true
    }
  };
  return capabilities[type] || {};
}

function calculateCampaignSummary(campaigns) {
  return {
    total_campaigns: campaigns.length,
    by_type: campaigns.reduce((acc, campaign) => {
      acc[campaign.type_name] = (acc[campaign.type_name] || 0) + 1;
      return acc;
    }, {}),
    active_campaigns: campaigns.filter(c => c.status === 'ENABLED').length,
    paused_campaigns: campaigns.filter(c => c.status === 'PAUSED').length,
    total_daily_budget: campaigns.reduce((sum, c) => sum + parseFloat(c.budget.daily_amount), 0).toFixed(2),
    total_spend: campaigns.reduce((sum, c) => sum + parseFloat(c.metrics.cost), 0).toFixed(2),
    campaigns_with_targeting_data: campaigns.filter(c => c.targeting?.geographic?.available).length,
    campaigns_with_keywords: campaigns.filter(c => c.keywords?.available).length
  };
}

// Export the main function (keep existing exports for compatibility)
module.exports = {
  getCampaignPerformance,
  getCampaignTargeting,  // Direct function reference, not wrapped
  getCampaignKeywords
};