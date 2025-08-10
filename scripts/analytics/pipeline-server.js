/**
 * Fast Pipeline Data API - MySQL Powered
 * /scripts/analytics/fast-pipeline-data.js
 * 
 * Lightning-fast queries using your 26,440 Google Ads records + HubSpot data
 */

/**
 * Get complete pipeline data using MySQL (FAST!)
 */
async function getFastPipelineData(getDbConnection, options = {}) {
  const { days = 30, campaign = 'all' } = options;
  
  try {
    console.log(`⚡ Fast pipeline data: ${days} days, campaign: ${campaign}`);
    
    const connection = await getDbConnection();
    
    try {
      // Get all data in parallel from MySQL (super fast!)
      const [mqlData, sqlData, campaignList, summary, trends] = await Promise.all([
        getMQLStagesFromMySQL(connection, options),
        getSQLStagesFromMySQL(connection, options),
        getCampaignListFromMySQL(connection, options),
        getPipelineSummaryFromMySQL(connection, options),
        getTrendsFromMySQL(connection, options)
      ]);
      
      return {
        success: true,
        summary,
        mqlStages: mqlData,
        sqlStages: sqlData,
        campaigns: campaignList,
        trends,
        period: `Last ${days} days`,
        dataSource: 'MySQL (26,440+ records)',
        performance: 'LIGHTNING FAST ⚡',
        timestamp: new Date().toISOString()
      };
      
    } finally {
      await connection.end();
    }
    
  } catch (error) {
    console.error('❌ Fast pipeline data failed:', error);
    return {
      success: false,
      error: error.message,
      timestamp: new Date().toISOString()
    };
  }
}

/**
 * Get MQL stages from Google Ads MySQL data (INSTANT!)
 */
async function getMQLStagesFromMySQL(connection, options) {
  const { days = 30, campaign = 'all' } = options;
  
  console.log('⚡ Getting MQL data from MySQL...');
  
  // Calculate date range
  const endDate = new Date();
  const startDate = new Date();
  startDate.setDate(startDate.getDate() - days);
  
  // Build campaign filter
  let campaignFilter = '';
  let params = [startDate.toISOString().split('T')[0], endDate.toISOString().split('T')[0]];
  
  if (campaign && campaign !== 'all') {
    campaignFilter = 'AND gc.campaign_name = ?';
    params.push(campaign);
  }
  
  // FAST MySQL query for Google Ads data
  const googleAdsQuery = `
    SELECT 
      SUM(gcm.impressions) as total_impressions,
      SUM(gcm.clicks) as total_clicks,
      SUM(gcm.cost_eur) as total_cost,
      SUM(gcm.conversions) as total_conversions,
      AVG(gcm.ctr) as avg_ctr,
      AVG(gcm.cpc_eur) as avg_cpc,
      COUNT(DISTINCT gcm.date) as days_with_data,
      COUNT(DISTINCT gcm.google_campaign_id) as active_campaigns
    FROM gads_campaign_metrics gcm
    JOIN gads_campaigns gc ON gcm.google_campaign_id = gc.google_campaign_id
    WHERE gcm.date >= ? 
      AND gcm.date <= ?
      AND gc.status = 2
      ${campaignFilter}
  `;
  
  const [googleAdsResults] = await connection.execute(googleAdsQuery, params);
  const gadsData = googleAdsResults[0] || {};
  
  // Get HubSpot contacts data (territory validation)
  const hubspotContactsQuery = `
    SELECT 
      COUNT(*) as total_contacts,
      COUNT(CASE WHEN cr.status = 'green' THEN 1 END) as accepted_contacts,
      COUNT(CASE WHEN cr.status IN ('yellow', 'red') OR cr.status IS NULL THEN 1 END) as rejected_contacts,
      COUNT(CASE WHEN hc.gclid IS NOT NULL THEN 1 END) as contacts_with_gclid
    FROM hub_contacts hc
    LEFT JOIN country_rules cr ON hc.country_code = cr.country_code
    WHERE hc.hs_object_source = 'PAID_SEARCH'
      AND hc.createdate >= ? 
      AND hc.createdate <= ?
      ${campaign !== 'all' ? 'AND (hc.google_ads_campaign = ? OR hc.hs_object_source_detail_1 = ?)' : ''}
  `;
  
  const hubspotParams = [startDate.toISOString().split('T')[0], endDate.toISOString().split('T')[0]];
  if (campaign !== 'all') {
    hubspotParams.push(campaign, campaign);
  }
  
  const [hubspotResults] = await connection.execute(hubspotContactsQuery, hubspotParams);
  const hubspotData = hubspotResults[0] || {};
  
  // Combine data for MQL stages
  const impressions = parseInt(gadsData.total_impressions) || 0;
  const clicks = parseInt(gadsData.total_clicks) || 0;
  const cost = parseFloat(gadsData.total_cost) || 0;
  const contacts = parseInt(hubspotData.total_contacts) || 0;
  const acceptedContacts = parseInt(hubspotData.accepted_contacts) || 0;
  const rejectedContacts = parseInt(hubspotData.rejected_contacts) || 0;
  
  const ctr = impressions > 0 ? ((clicks / impressions) * 100) : 0;
  const conversionRate = clicks > 0 ? ((contacts / clicks) * 100) : 0;
  const rejectionRate = contacts > 0 ? ((rejectedContacts / contacts) * 100) : 0;
  
  console.log(`⚡ MQL data: ${impressions} impressions → ${clicks} clicks → ${contacts} contacts`);
  
  return {
    impressions: { 
      count: impressions, 
      cost: 0,
      source: 'MySQL Google Ads'
    },
    clicks: { 
      count: clicks, 
      cost: Math.round(cost * 0.8), 
      ctr: parseFloat(ctr.toFixed(2)),
      source: 'MySQL Google Ads'
    },
    ctaComplete: { 
      count: contacts, 
      cost: cost, 
      conversionRate: parseFloat(conversionRate.toFixed(2)),
      source: 'MySQL HubSpot'
    },
    territoryValidation: {
      accepted: acceptedContacts,
      rejected: rejectedContacts,
      rejectionRate: parseFloat(rejectionRate.toFixed(2)),
      cost: cost,
      source: 'MySQL HubSpot + country_rules'
    }
  };
}

/**
 * Get SQL stages from HubSpot MySQL data (INSTANT!)
 */
async function getSQLStagesFromMySQL(connection, options) {
  const { days = 30, campaign = 'all' } = options;
  
  console.log('⚡ Getting SQL data from MySQL...');
  
  // Calculate date range
  const endDate = new Date();
  const startDate = new Date();
  startDate.setDate(startDate.getDate() - days);
  
  // Build campaign filter
  let campaignFilter = '';
  let params = [startDate.toISOString().split('T')[0], endDate.toISOString().split('T')[0]];
  
  if (campaign && campaign !== 'all') {
    campaignFilter = 'AND (hc.google_ads_campaign = ? OR hc.hs_object_source_detail_1 = ?)';
    params.push(campaign, campaign);
  }
  
  // Get total PAID_SEARCH contacts for percentages
  const totalContactsQuery = `
    SELECT COUNT(*) as total_contacts
    FROM hub_contacts hc
    WHERE hc.hs_object_source = 'PAID_SEARCH'
      AND hc.createdate >= ? 
      AND hc.createdate <= ?
      ${campaignFilter}
  `;
  
  const [totalResults] = await connection.execute(totalContactsQuery, params);
  const totalContacts = totalResults[0]?.total_contacts || 0;
  
  // Query SQL stages (deals)
  const dealQuery = `
    SELECT 
      hd.dealstage,
      COUNT(DISTINCT hc.hubspot_id) as contact_count,
      COUNT(DISTINCT hd.hubspot_deal_id) as deal_count,
      SUM(CASE WHEN hd.amount > 0 THEN hd.amount ELSE 0 END) as total_revenue
    FROM hub_contacts hc
    JOIN hub_contact_deal_associations hda ON hc.hubspot_id = hda.contact_hubspot_id
    JOIN hub_deals hd ON hda.deal_hubspot_id = hd.hubspot_deal_id
    WHERE hc.hs_object_source = 'PAID_SEARCH'
      AND hc.createdate >= ? 
      AND hc.createdate <= ?
      AND hd.pipeline = 'default'
      ${campaignFilter}
    GROUP BY hd.dealstage
    ORDER BY 
      CASE hd.dealstage
        WHEN 'appointmentscheduled' THEN 1
        WHEN '113151423' THEN 2
        WHEN 'qualifiedtobuy' THEN 3  
        WHEN '767120827' THEN 4
        WHEN 'presentationscheduled' THEN 5
        WHEN 'decisionmakerboughtin' THEN 6
        WHEN 'contractsent' THEN 7
        WHEN 'closedwon' THEN 8
        WHEN 'closedlost' THEN 9
        ELSE 10
      END
  `;
  
  const [dealResults] = await connection.execute(dealQuery, params);
  
  // Map HubSpot stage IDs to friendly names
  const stageMapping = {
    'appointmentscheduled': 'inbox',
    '113151423': 'sequenced', 
    'qualifiedtobuy': 'engaging',
    '767120827': 'responsive',
    'presentationscheduled': 'advising',
    'decisionmakerboughtin': 'consideration',
    'contractsent': 'contract',
    'closedwon': 'won',
    'closedlost': 'lost'
  };
  
  const sqlStages = {};
  
  // Initialize all stages
  Object.values(stageMapping).forEach(stage => {
    sqlStages[stage] = { count: 0, percentage: 0 };
  });
  
  // Populate with real data
  dealResults.forEach(deal => {
    const friendlyStage = stageMapping[deal.dealstage] || deal.dealstage;
    if (sqlStages[friendlyStage]) {
      sqlStages[friendlyStage].count = deal.contact_count;
      sqlStages[friendlyStage].percentage = totalContacts > 0 ? 
        parseFloat(((deal.contact_count / totalContacts) * 100).toFixed(2)) : 0;
    }
  });
  
  console.log(`⚡ SQL data: ${totalContacts} total contacts across ${dealResults.length} deal stages`);
  
  return sqlStages;
}

/**
 * Get campaign list from MySQL (INSTANT!)
 */
async function getCampaignListFromMySQL(connection, options) {
  const { days = 30 } = options;
  
  // Get campaigns with recent activity
  const campaignQuery = `
    SELECT 
      gc.campaign_name,
      gc.campaign_type_name,
      gc.status,
      COUNT(gcm.date) as days_with_data,
      SUM(gcm.impressions) as total_impressions,
      SUM(gcm.clicks) as total_clicks,
      SUM(gcm.cost_eur) as total_cost,
      MAX(gcm.date) as last_activity_date
    FROM gads_campaigns gc
    LEFT JOIN gads_campaign_metrics gcm ON gc.google_campaign_id = gcm.google_campaign_id
      AND gcm.date >= DATE_SUB(CURDATE(), INTERVAL ? DAY)
    WHERE gc.status = 2
    GROUP BY gc.google_campaign_id, gc.campaign_name, gc.campaign_type_name, gc.status
    HAVING days_with_data > 0
    ORDER BY total_cost DESC
    LIMIT 20
  `;
  
  const [results] = await connection.execute(campaignQuery, [days]);
  
  // Add "All Campaigns" option
  const campaigns = [
    { id: 'all', name: 'All Campaigns', type: 'All Types' }
  ];
  
  results.forEach(row => {
    campaigns.push({
      id: row.campaign_name,
      name: row.campaign_name,
      type: row.campaign_type_name,
      cost: parseFloat(row.total_cost),
      impressions: parseInt(row.total_impressions),
      clicks: parseInt(row.total_clicks),
      days_active: row.days_with_data,
      last_activity: row.last_activity_date
    });
  });
  
  return campaigns;
}

/**
 * Get pipeline summary from MySQL (INSTANT!)
 */
async function getPipelineSummaryFromMySQL(connection, options) {
  const { days = 30, campaign = 'all' } = options;
  
  // Calculate date range
  const endDate = new Date();
  const startDate = new Date();
  startDate.setDate(startDate.getDate() - days);
  
  // Build campaign filter
  let campaignFilter = '';
  let params = [startDate.toISOString().split('T')[0], endDate.toISOString().split('T')[0]];
  
  if (campaign && campaign !== 'all') {
    campaignFilter = 'AND gc.campaign_name = ?';
    params.push(campaign);
  }
  
  // Get Google Ads summary
  const gadsQuery = `
    SELECT 
      SUM(gcm.cost_eur) as total_cost,
      COUNT(DISTINCT gcm.google_campaign_id) as active_campaigns,
      AVG(gcm.cpc_eur) as avg_cpc
    FROM gads_campaign_metrics gcm
    JOIN gads_campaigns gc ON gcm.google_campaign_id = gc.google_campaign_id
    WHERE gcm.date >= ? 
      AND gcm.date <= ?
      AND gc.status = 2
      ${campaignFilter}
  `;
  
  const [gadsResults] = await connection.execute(gadsQuery, params);
  const gadsData = gadsResults[0] || {};
  
  // Get HubSpot summary
  const hubspotParams = [startDate.toISOString().split('T')[0], endDate.toISOString().split('T')[0]];
  if (campaign !== 'all') {
    hubspotParams.push(campaign, campaign);
  }
  
  const hubspotQuery = `
    SELECT 
      COUNT(*) as total_contacts,
      COUNT(CASE WHEN hda.deal_hubspot_id IS NOT NULL THEN 1 END) as contacts_with_deals
    FROM hub_contacts hc
    LEFT JOIN hub_contact_deal_associations hda ON hc.hubspot_id = hda.contact_hubspot_id
    WHERE hc.hs_object_source = 'PAID_SEARCH'
      AND hc.createdate >= ? 
      AND hc.createdate <= ?
      ${campaign !== 'all' ? 'AND (hc.google_ads_campaign = ? OR hc.hs_object_source_detail_1 = ?)' : ''}
  `;
  
  const [hubspotResults] = await connection.execute(hubspotQuery, hubspotParams);
  const hubspotData = hubspotResults[0] || {};
  
  const totalContacts = parseInt(hubspotData.total_contacts) || 0;
  const contactsWithDeals = parseInt(hubspotData.contacts_with_deals) || 0;
  const totalCost = parseFloat(gadsData.total_cost) || 0;
  
  return {
    campaign: campaign === 'all' ? 'All Campaigns' : campaign,
    totalContacts,
    period: `Last ${days} days`,
    audience: 'PAID_SEARCH contacts',
    totalCost: totalCost,
    costPerContact: totalContacts > 0 ? parseFloat((totalCost / totalContacts).toFixed(2)) : 0,
    conversionRate: totalContacts > 0 ? parseFloat(((contactsWithDeals / totalContacts) * 100).toFixed(2)) : 0,
    activeCampaigns: parseInt(gadsData.active_campaigns) || 0,
    avgCPC: parseFloat(gadsData.avg_cpc) || 0,
    dataQuality: {
      googleAdsRecords: '26,440+ metrics',
      hubspotContacts: totalContacts,
      dataSource: 'MySQL Lightning Fast ⚡'
    }
  };
}

/**
 * Get trends from MySQL (INSTANT!)
 */
async function getTrendsFromMySQL(connection, options) {
  const { days = 30, campaign = 'all' } = options;
  
  // Build campaign filter
  let campaignFilter = '';
  let params = [days];
  
  if (campaign && campaign !== 'all') {
    campaignFilter = 'AND gc.campaign_name = ?';
    params.push(campaign);
  }
  
  const trendsQuery = `
    SELECT 
      gcm.date,
      SUM(gcm.impressions) as impressions,
      SUM(gcm.clicks) as clicks,
      SUM(gcm.cost_eur) as cost,
      SUM(gcm.conversions) as conversions,
      AVG(gcm.ctr) as ctr
    FROM gads_campaign_metrics gcm
    JOIN gads_campaigns gc ON gcm.google_campaign_id = gc.google_campaign_id
    WHERE gcm.date >= DATE_SUB(CURDATE(), INTERVAL ? DAY)
      AND gc.status = 2
      ${campaignFilter}
    GROUP BY gcm.date
    ORDER BY gcm.date DESC
    LIMIT 30
  `;
  
  const [results] = await connection.execute(trendsQuery, params);
  
  return results.map(row => ({
    date: row.date,
    impressions: parseInt(row.impressions) || 0,
    clicks: parseInt(row.clicks) || 0,
    cost: parseFloat(row.cost) || 0,
    conversions: parseFloat(row.conversions) || 0,
    ctr: parseFloat(row.ctr) || 0
  }));
}

module.exports = {
  getFastPipelineData
};