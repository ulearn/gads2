/**
 * Google Ads Sync Module - Smart Historical & Live Data
 * /scripts/google/gads-sync.js
 * 
 * Syncs Google Ads data to MySQL with intelligent prioritization:
 * - Active campaigns: Daily sync with full metrics
 * - Paused campaigns: Sync once, then skip unless reactivated
 * - Historical backfill: Gradual sync to avoid API limits
 */

/**
 * Main sync function - handles different sync strategies
 * @param {Object} customer - Google Ads customer client
 * @param {Function} getDbConnection - Database connection function
 * @param {Object} options - Sync options
 */
async function syncGoogleAdsData(customer, getDbConnection, options = {}) {
  const { 
    syncType = 'incremental', // 'full', 'incremental', 'backfill'
    days = 7,
    backfillDays = 30,
    forceSync = false
  } = options;
  
  let connection;
  
  try {
    console.log(`🔄 Starting Google Ads sync: ${syncType} (${days} days)`);
    
    connection = await getDbConnection();
    
    // Log sync start
    const syncLogId = await logSyncStart(connection, syncType, options);
    
    let result;
    
    switch (syncType) {
      case 'full':
        result = await fullSync(customer, connection, options);
        break;
      case 'incremental':
        result = await incrementalSync(customer, connection, options);
        break;
      case 'backfill':
        result = await backfillSync(customer, connection, options);
        break;
      case 'campaigns-only':
        result = await syncCampaignsOnly(customer, connection, options);
        break;
      default:
        throw new Error(`Unknown sync type: ${syncType}`);
    }
    
    // Log sync completion
    await logSyncComplete(connection, syncLogId, result);
    
    console.log(`✅ Google Ads sync completed: ${JSON.stringify(result.summary)}`);
    
    return {
      success: true,
      syncType,
      result,
      timestamp: new Date().toISOString()
    };
    
  } catch (error) {
    console.error(`❌ Google Ads sync failed (${syncType}):`, error);
    
    if (connection) {
      await logSyncError(connection, error.message);
    }
    
    return {
      success: false,
      syncType,
      error: error.message,
      timestamp: new Date().toISOString()
    };
  } finally {
    if (connection) {
      await connection.end();
    }
  }
}

/**
 * Full sync - campaigns, targeting, keywords, and recent metrics
 */
async function fullSync(customer, connection, options) {
  console.log('📊 Full sync: campaigns + targeting + keywords + metrics');
  
  const summary = {
    campaigns_synced: 0,
    metrics_synced: 0,
    keywords_synced: 0,
    targeting_synced: 0,
    api_calls_used: 0
  };
  
  // Step 1: Sync all campaigns
  console.log('📋 Syncing campaigns...');
  const campaignResult = await syncCampaigns(customer, connection, syncLogId);
  summary.campaigns_synced = campaignResult.campaigns_synced;
  summary.api_calls_used += campaignResult.api_calls_used;
  
  // Step 2: Sync targeting for all campaigns
  console.log('🎯 Syncing geographic targeting...');
  const targetingResult = await syncGeographicTargeting(customer, connection);
  summary.targeting_synced = targetingResult.targeting_synced;
  summary.api_calls_used += targetingResult.api_calls_used;
  
  // Step 3: Sync keywords for Search campaigns only
  console.log('🔍 Syncing keywords for Search campaigns...');
  const keywordResult = await syncKeywords(customer, connection);
  summary.keywords_synced = keywordResult.keywords_synced;
  summary.api_calls_used += keywordResult.api_calls_used;
  
  // Step 4: Sync recent metrics for active campaigns
  console.log('📈 Syncing recent metrics for active campaigns...');
  const metricsResult = await syncMetricsForActiveCampaigns(customer, connection, options.days || 30);
  summary.metrics_synced = metricsResult.metrics_synced;
  summary.api_calls_used += metricsResult.api_calls_used;
  
  return { summary };
}

/**
 * Incremental sync - daily metrics for active campaigns
 */
async function incrementalSync(customer, connection, options) {
  console.log('⚡ Incremental sync: daily metrics for active campaigns');
  
  const summary = {
    campaigns_checked: 0,
    metrics_synced: 0,
    api_calls_used: 0
  };
  
  // Only sync metrics for active campaigns from recent days
  const metricsResult = await syncMetricsForActiveCampaigns(customer, connection, options.days || 7);
  summary.metrics_synced = metricsResult.metrics_synced;
  summary.api_calls_used = metricsResult.api_calls_used;
  
  // Update campaign status if any campaigns changed
  const statusResult = await updateCampaignStatuses(customer, connection);
  summary.campaigns_checked = statusResult.campaigns_checked;
  summary.api_calls_used += statusResult.api_calls_used;
  
  return { summary };
}

/**
 * Backfill sync - historical data for specific date ranges
 */
async function backfillSync(customer, connection, options) {
  console.log(`📚 Backfill sync: ${options.backfillDays || 90} days of historical data`);
  
  const summary = {
    campaigns_synced: 0,
    metrics_synced: 0,
    api_calls_used: 0
  };
  
  // Get active campaigns only for backfill
  const activeCampaigns = await getActiveCampaigns(connection);
  
  if (activeCampaigns.length === 0) {
    console.log('⚠️ No active campaigns found for backfill');
    return { summary };
  }
  
  console.log(`📊 Backfilling ${options.backfillDays || 90} days for ${activeCampaigns.length} active campaigns`);
  
  const metricsResult = await syncHistoricalMetrics(
    customer, 
    connection, 
    activeCampaigns, 
    options.backfillDays || 90
  );
  
  summary.metrics_synced = metricsResult.metrics_synced;
  summary.api_calls_used = metricsResult.api_calls_used;
  
  return { summary };
}

/**
 * Sync campaigns only - structure and status
 */
async function syncCampaignsOnly(customer, connection, options) {
  console.log('📋 Campaigns-only sync: structure and status');
  
  const result = await syncCampaigns(customer, connection);
  
  return {
    summary: {
      campaigns_synced: result.campaigns_synced,
      api_calls_used: result.api_calls_used
    }
  };
}

/**
 * Sync all campaigns from Google Ads API
 */
async function syncCampaigns(customer, connection, syncId = null) {
  try {
    console.log('📋 Fetching all campaigns from Google Ads...');
    
    const query = `
      SELECT 
        campaign.id,
        campaign.name,
        campaign.status,
        campaign.advertising_channel_type,
        campaign.start_date,
        campaign.end_date,
        campaign.bidding_strategy_type,
        campaign_budget.id,
        campaign_budget.name,
        campaign_budget.amount_micros
      FROM campaign 
      ORDER BY campaign.name
    `;
    
    const results = await customer.query(query);
    console.log(`📊 Found ${results.length} campaigns in Google Ads`);
    
    let campaignsSynced = 0;
    
    for (const row of results) {
      const campaign = row.campaign;
      const budget = row.campaign_budget;
      
      const campaignData = {
        google_campaign_id: campaign.id?.toString(),
        campaign_name: campaign.name,
        campaign_type: campaign.advertising_channel_type,
        campaign_type_name: getCampaignTypeName(campaign.advertising_channel_type),
        status: campaign.status,
        start_date: campaign.start_date || null,
        end_date: campaign.end_date || null,
        bidding_strategy: campaign.bidding_strategy_type,
        budget_id: budget?.id?.toString(),
        budget_name: budget?.name,
        daily_budget_micros: budget?.amount_micros || 0,
        daily_budget_eur: budget?.amount_micros ? (budget.amount_micros / 1000000) : 0
      };
      
      await upsertCampaign(connection, campaignData, syncId);
      campaignsSynced++;
    }
    
    console.log(`✅ Synced ${campaignsSynced} campaigns`);
    
    return {
      campaigns_synced: campaignsSynced,
      api_calls_used: 1
    };
    
  } catch (error) {
    console.error('❌ Campaign sync failed:', error);
    throw error;
  }
}

/**
 * Sync metrics for active campaigns only (smart filtering)
 */
async function syncMetricsForActiveCampaigns(customer, connection, days = 7) {
  try {
    console.log(`📈 Syncing ${days} days of metrics for active campaigns...`);
    
    // Get active campaigns from our database
    const activeCampaigns = await getActiveCampaigns(connection);
    
    if (activeCampaigns.length === 0) {
      console.log('⚠️ No active campaigns found');
      return { metrics_synced: 0, api_calls_used: 0 };
    }
    
    console.log(`📊 Found ${activeCampaigns.length} active campaigns`);
    
    // Query metrics for active campaigns only
    const campaignIds = activeCampaigns.map(c => c.google_campaign_id).join(',');
    
    const metricsQuery = `
      SELECT 
        campaign.id,
        segments.date,
        metrics.impressions,
        metrics.clicks,
        metrics.ctr,
        metrics.average_cpc,
        metrics.cost_micros,
        metrics.conversions,
        metrics.view_through_conversions
      FROM campaign
      WHERE campaign.id IN (${campaignIds})
        AND segments.date DURING LAST_${days}_DAYS
        AND campaign.status = 'ENABLED'
      ORDER BY segments.date DESC, campaign.id
    `;
    
    console.log(`📊 Fetching metrics for ${activeCampaigns.length} campaigns, ${days} days...`);
    const results = await customer.query(metricsQuery);
    
    console.log(`📈 Processing ${results.length} metric records...`);
    
    let metricsSynced = 0;
    
    for (const row of results) {
      const metricsData = {
        google_campaign_id: row.campaign.id?.toString(),
        date: row.segments.date,
        impressions: row.metrics?.impressions || 0,
        clicks: row.metrics?.clicks || 0,
        cost_micros: row.metrics?.cost_micros || 0,
        cost_eur: row.metrics?.cost_micros ? (row.metrics.cost_micros / 1000000) : 0,
        conversions: row.metrics?.conversions || 0,
        view_through_conversions: row.metrics?.view_through_conversions || 0,
        ctr: row.metrics?.ctr ? (row.metrics.ctr * 100) : 0,
        cpc_micros: row.metrics?.average_cpc || 0,
        cpc_eur: row.metrics?.average_cpc ? (row.metrics.average_cpc / 1000000) : 0,
        conversion_rate: (row.metrics?.clicks > 0 && row.metrics?.conversions > 0) ? 
          ((row.metrics.conversions / row.metrics.clicks) * 100) : 0,
        cost_per_conversion_eur: (row.metrics?.conversions > 0 && row.metrics?.cost_micros > 0) ? 
          ((row.metrics.cost_micros / 1000000) / row.metrics.conversions) : 0
      };
      
      await upsertCampaignMetrics(connection, metricsData);
      metricsSynced++;
    }
    
    console.log(`✅ Synced ${metricsSynced} metric records`);
    
    return {
      metrics_synced: metricsSynced,
      api_calls_used: 1
    };
    
  } catch (error) {
    console.error('❌ Metrics sync failed:', error);
    throw error;
  }
}

/**
 * Sync geographic targeting for campaigns
 */
async function syncGeographicTargeting(customer, connection) {
  try {
    console.log('🎯 Syncing geographic targeting...');
    
    const targetingQuery = `
      SELECT 
        campaign_criterion.campaign,
        campaign_criterion.criterion_id,
        campaign_criterion.location.geo_target_constant,
        campaign_criterion.negative
      FROM campaign_criterion
      WHERE campaign_criterion.type = 'LOCATION'
      ORDER BY campaign_criterion.campaign
    `;
    
    const results = await customer.query(targetingQuery);
    console.log(`📍 Found ${results.length} location criteria`);
    
    // Get location details in batches to avoid overwhelming the API
    const geoTargetConstants = [...new Set(
      results
        .map(row => row.campaign_criterion.location?.geo_target_constant)
        .filter(id => id)
        .map(id => id.replace('geoTargetConstants/', ''))
    )];
    
    const locationDetails = new Map();
    
    if (geoTargetConstants.length > 0) {
      // Batch location queries (max 50 at a time)
      const batchSize = 50;
      for (let i = 0; i < geoTargetConstants.length; i += batchSize) {
        const batch = geoTargetConstants.slice(i, i + batchSize);
        
        const locationQuery = `
          SELECT 
            geo_target_constant.id,
            geo_target_constant.canonical_name,
            geo_target_constant.country_code,
            geo_target_constant.target_type
          FROM geo_target_constant
          WHERE geo_target_constant.id IN (${batch.join(',')})
        `;
        
        const locationResults = await customer.query(locationQuery);
        
        locationResults.forEach(row => {
          const geo = row.geo_target_constant;
          locationDetails.set(geo.id?.toString(), {
            name: geo.canonical_name,
            country_code: geo.country_code,
            target_type: geo.target_type
          });
        });
      }
    }
    
    let targetingSynced = 0;
    
    // Insert targeting data
    for (const row of results) {
      const criterion = row.campaign_criterion;
      const geoTargetId = criterion.location?.geo_target_constant?.replace('geoTargetConstants/', '');
      const locationInfo = locationDetails.get(geoTargetId) || {};
      
      const targetingData = {
        google_campaign_id: criterion.campaign,
        geo_target_constant: geoTargetId,
        location_name: locationInfo.name || `Unknown (${geoTargetId})`,
        country_code: locationInfo.country_code,
        target_type: locationInfo.target_type,
        is_negative: criterion.negative ? 1 : 0
      };
      
      await upsertGeoTargeting(connection, targetingData);
      targetingSynced++;
    }
    
    console.log(`✅ Synced ${targetingSynced} targeting criteria`);
    
    return {
      targeting_synced: targetingSynced,
      api_calls_used: Math.ceil(geoTargetConstants.length / 50) + 1
    };
    
  } catch (error) {
    console.error('❌ Targeting sync failed:', error);
    throw error;
  }
}

/**
 * Sync keywords for Search campaigns only
 */
async function syncKeywords(customer, connection) {
  try {
    console.log('🔍 Syncing keywords for Search campaigns...');
    
    // Get Search campaigns only
    const searchCampaigns = await getSearchCampaigns(connection);
    
    if (searchCampaigns.length === 0) {
      console.log('⚠️ No Search campaigns found');
      return { keywords_synced: 0, api_calls_used: 0 };
    }
    
    const campaignIds = searchCampaigns.map(c => c.google_campaign_id).join(',');
    
    const keywordsQuery = `
      SELECT 
        campaign.id,
        ad_group.id,
        ad_group_criterion.keyword.text,
        ad_group_criterion.keyword.match_type,
        ad_group_criterion.status
      FROM keyword_view
      WHERE campaign.id IN (${campaignIds})
        AND ad_group_criterion.status != 'REMOVED'
      ORDER BY campaign.id, ad_group.id
    `;
    
    const results = await customer.query(keywordsQuery);
    console.log(`🔍 Found ${results.length} keywords`);
    
    let keywordsSynced = 0;
    
    for (const row of results) {
      const keywordData = {
        google_campaign_id: row.campaign.id?.toString(),
        google_adgroup_id: row.ad_group.id?.toString(),
        keyword_text: row.ad_group_criterion.keyword?.text,
        match_type: row.ad_group_criterion.keyword?.match_type,
        match_type_name: getMatchTypeName(row.ad_group_criterion.keyword?.match_type),
        status: row.ad_group_criterion.status
      };
      
      await upsertKeyword(connection, keywordData);
      keywordsSynced++;
    }
    
    console.log(`✅ Synced ${keywordsSynced} keywords`);
    
    return {
      keywords_synced: keywordsSynced,
      api_calls_used: 1
    };
    
  } catch (error) {
    console.error('❌ Keywords sync failed:', error);
    throw error;
  }
}

/**
 * Database helper functions
 */

async function getActiveCampaigns(connection) {
  const [results] = await connection.execute(`
    SELECT google_campaign_id, campaign_name 
    FROM gads_campaigns 
    WHERE status = 2
    ORDER BY campaign_name
  `);
  return results;
}

async function getSearchCampaigns(connection) {
  const [results] = await connection.execute(`
    SELECT google_campaign_id, campaign_name 
    FROM gads_campaigns 
    WHERE campaign_type = 2 AND status = 2
    ORDER BY campaign_name
  `);
  return results;
}

async function upsertCampaign(connection, data, syncId = null) {
  // First, check if campaign exists and get current status
  const [existing] = await connection.execute(`
    SELECT google_campaign_id, status, campaign_name 
    FROM gads_campaigns 
    WHERE google_campaign_id = ?
  `, [data.google_campaign_id]);
  
  const existingCampaign = existing[0];
  const statusChanged = existingCampaign && existingCampaign.status !== data.status;
  
  // Insert/update campaign
  const query = `
    INSERT INTO gads_campaigns (
      google_campaign_id, campaign_name, campaign_type, campaign_type_name,
      status, start_date, end_date, bidding_strategy, budget_id, budget_name,
      daily_budget_micros, daily_budget_eur
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON DUPLICATE KEY UPDATE
      campaign_name = VALUES(campaign_name),
      status = VALUES(status),
      budget_id = VALUES(budget_id),
      budget_name = VALUES(budget_name),
      daily_budget_micros = VALUES(daily_budget_micros),
      daily_budget_eur = VALUES(daily_budget_eur),
      updated_at = CURRENT_TIMESTAMP
  `;
  
  await connection.execute(query, [
    data.google_campaign_id, data.campaign_name, data.campaign_type, data.campaign_type_name,
    data.status, data.start_date, data.end_date, data.bidding_strategy, data.budget_id, 
    data.budget_name, data.daily_budget_micros, data.daily_budget_eur
  ]);
  
  // Track status change if detected
  if (statusChanged) {
    console.log(`📊 Status change detected: ${data.campaign_name} (${getStatusName(existingCampaign.status)} → ${getStatusName(data.status)})`);
    
    await trackStatusChange(connection, {
      google_campaign_id: data.google_campaign_id,
      campaign_name: data.campaign_name,
      old_status: existingCampaign.status,
      new_status: data.status,
      sync_id: syncId
    });
  } else if (!existingCampaign) {
    // New campaign - record initial status
    console.log(`📊 New campaign detected: ${data.campaign_name} (${getStatusName(data.status)})`);
    
    await trackStatusChange(connection, {
      google_campaign_id: data.google_campaign_id,
      campaign_name: data.campaign_name,
      old_status: null,
      new_status: data.status,
      sync_id: syncId
    });
  }
}

/**
 * Track campaign status changes
 */
async function trackStatusChange(connection, data) {
  const query = `
    INSERT INTO gads_campaign_status_history (
      google_campaign_id, campaign_name, old_status, new_status,
      old_status_name, new_status_name, detected_by_sync, sync_id
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
  `;
  
  await connection.execute(query, [
    data.google_campaign_id,
    data.campaign_name,
    data.old_status,
    data.new_status,
    data.old_status ? getStatusName(data.old_status) : null,
    getStatusName(data.new_status),
    'auto-sync',
    data.sync_id
  ]);
}

/**
 * Get human-readable status name
 */
function getStatusName(status) {
  const statusNames = {
    2: 'ENABLED',
    3: 'PAUSED', 
    4: 'REMOVED',
    5: 'DRAFT'
  };
  return statusNames[status] || `UNKNOWN (${status})`;
}

async function upsertCampaignMetrics(connection, data) {
  const query = `
    INSERT INTO gads_campaign_metrics (
      google_campaign_id, date, impressions, clicks, cost_micros, cost_eur,
      conversions, view_through_conversions, ctr, cpc_micros, cpc_eur,
      conversion_rate, cost_per_conversion_eur
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON DUPLICATE KEY UPDATE
      impressions = VALUES(impressions),
      clicks = VALUES(clicks),
      cost_micros = VALUES(cost_micros),
      cost_eur = VALUES(cost_eur),
      conversions = VALUES(conversions),
      view_through_conversions = VALUES(view_through_conversions),
      ctr = VALUES(ctr),
      cpc_micros = VALUES(cpc_micros),
      cpc_eur = VALUES(cpc_eur),
      conversion_rate = VALUES(conversion_rate),
      cost_per_conversion_eur = VALUES(cost_per_conversion_eur),
      synced_at = CURRENT_TIMESTAMP
  `;
  
  await connection.execute(query, [
    data.google_campaign_id, data.date, data.impressions, data.clicks,
    data.cost_micros, data.cost_eur, data.conversions, data.view_through_conversions,
    data.ctr, data.cpc_micros, data.cpc_eur, data.conversion_rate, data.cost_per_conversion_eur
  ]);
}

async function upsertGeoTargeting(connection, data) {
  const query = `
    INSERT INTO gads_geo_targeting (
      google_campaign_id, geo_target_constant, location_name, country_code,
      target_type, is_negative
    ) VALUES (?, ?, ?, ?, ?, ?)
    ON DUPLICATE KEY UPDATE
      location_name = VALUES(location_name),
      country_code = VALUES(country_code),
      target_type = VALUES(target_type),
      is_negative = VALUES(is_negative)
  `;
  
  await connection.execute(query, [
    data.google_campaign_id, data.geo_target_constant, data.location_name,
    data.country_code, data.target_type, data.is_negative
  ]);
}

async function upsertKeyword(connection, data) {
  const query = `
    INSERT INTO gads_keywords (
      google_campaign_id, google_adgroup_id, keyword_text, match_type,
      match_type_name, status
    ) VALUES (?, ?, ?, ?, ?, ?)
    ON DUPLICATE KEY UPDATE
      status = VALUES(status),
      updated_at = CURRENT_TIMESTAMP
  `;
  
  await connection.execute(query, [
    data.google_campaign_id, data.google_adgroup_id, data.keyword_text,
    data.match_type, data.match_type_name, data.status
  ]);
}

/**
 * Sync logging functions
 */
async function logSyncStart(connection, syncType, options) {
  const query = `
    INSERT INTO gads_sync_log (sync_type, start_date, end_date, status)
    VALUES (?, ?, ?, 'running')
  `;
  
  const startDate = options.startDate || null;
  const endDate = options.endDate || null;
  
  const [result] = await connection.execute(query, [syncType, startDate, endDate]);
  return result.insertId;
}

async function logSyncComplete(connection, syncLogId, result) {
  const query = `
    UPDATE gads_sync_log 
    SET status = 'completed',
        campaigns_synced = ?,
        metrics_synced = ?,
        keywords_synced = ?,
        api_calls_used = ?,
        completed_at = CURRENT_TIMESTAMP
    WHERE sync_id = ?
  `;
  
  await connection.execute(query, [
    result.summary.campaigns_synced || 0,
    result.summary.metrics_synced || 0,
    result.summary.keywords_synced || 0,
    result.summary.api_calls_used || 0,
    syncLogId
  ]);
}

async function logSyncError(connection, errorMessage) {
  const query = `
    UPDATE gads_sync_log 
    SET status = 'failed', error_message = ?, completed_at = CURRENT_TIMESTAMP
    WHERE status = 'running'
    ORDER BY sync_id DESC 
    LIMIT 1
  `;
  
  await connection.execute(query, [errorMessage]);
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

function getMatchTypeName(type) {
  const matchTypes = {
    1: 'EXACT',
    2: 'PHRASE', 
    3: 'BROAD',
    4: 'BROAD_MATCH_MODIFIER'
  };
  return matchTypes[type] || `Unknown (${type})`;
}

/**
 * Update campaign statuses without full sync
 */
async function updateCampaignStatuses(customer, connection) {
  try {
    const query = `
      SELECT 
        campaign.id,
        campaign.status
      FROM campaign
    `;
    
    const results = await customer.query(query);
    
    let campaignsChecked = 0;
    
    for (const row of results) {
      const updateQuery = `
        UPDATE gads_campaigns 
        SET status = ?, updated_at = CURRENT_TIMESTAMP
        WHERE google_campaign_id = ?
      `;
      
      await connection.execute(updateQuery, [row.campaign.status, row.campaign.id?.toString()]);
      campaignsChecked++;
    }
    
    return {
      campaigns_checked: campaignsChecked,
      api_calls_used: 1
    };
    
  } catch (error) {
    console.error('❌ Status update failed:', error);
    throw error;
  }
}

/**
 * Route handlers for index.js (NO BUSINESS LOGIC IN INDEX!)
 */

/**
 * Handle full sync route
 */
async function handleFullSync(req, res, customer, getDbConnection) {
  try {
    console.log('🔄 Starting full Google Ads sync...');
    
    if (!customer) {
      throw new Error('Failed to initialize Google Ads client');
    }
    
    // Return immediately, run sync in background
    res.json({
      success: true,
      service: 'Google Ads Sync',
      message: 'Full sync started in background',
      status: 'running',
      timestamp: new Date().toISOString()
    });
    
    // Run sync in background
    (async () => {
      try {
        const result = await syncGoogleAdsData(customer, getDbConnection, {
          syncType: 'full',
          days: 30
        });
        console.log('✅ Background full sync completed:', result);
      } catch (error) {
        console.error('❌ Background sync failed:', error.message);
      }
    })();
    
  } catch (error) {
    console.error('❌ Google Ads full sync failed:', error.message);
    res.status(500).json({
      success: false,
      service: 'Google Ads Sync',
      error: error.message,
      timestamp: new Date().toISOString()
    });
  }
}

/**
 * Handle incremental sync route
 */
async function handleIncrementalSync(req, res, customer, getDbConnection) {
  try {
    const days = parseInt(req.query.days) || 7;
    console.log(`🔄 Starting incremental Google Ads sync (${days} days)...`);
    
    const result = await syncGoogleAdsData(customer, getDbConnection, {
      syncType: 'incremental',
      days: days
    });
    
    res.json({
      success: true,
      service: 'Google Ads Sync',
      message: 'Incremental sync completed',
      result: result,
      timestamp: new Date().toISOString()
    });
    
  } catch (error) {
    console.error('❌ Google Ads incremental sync failed:', error.message);
    res.status(500).json({
      success: false,
      service: 'Google Ads Sync',
      error: error.message,
      timestamp: new Date().toISOString()
    });
  }
}

/**
 * Handle campaigns-only sync route
 */
async function handleCampaignsSync(req, res, customer, getDbConnection) {
  try {
    console.log('🔄 Starting campaigns-only sync...');
    
    const result = await syncGoogleAdsData(customer, getDbConnection, {
      syncType: 'campaigns-only'
    });
    
    res.json({
      success: true,
      service: 'Google Ads Sync',
      message: 'Campaigns sync completed',
      result: result,
      timestamp: new Date().toISOString()
    });
    
  } catch (error) {
    console.error('❌ Google Ads campaigns sync failed:', error.message);
    res.status(500).json({
      success: false,
      service: 'Google Ads Sync',
      error: error.message,
      timestamp: new Date().toISOString()
    });
  }
}

/**
 * Handle sync status route
 */
async function handleSyncStatus(req, res, getDbConnection) {
  try {
    const connection = await getDbConnection();
    
    try {
      const [syncHistory] = await connection.execute(`
        SELECT 
          sync_id, sync_type, start_date, end_date, campaigns_synced,
          metrics_synced, keywords_synced, status, api_calls_used,
          started_at, completed_at, error_message
        FROM gads_sync_log
        ORDER BY sync_id DESC
        LIMIT 10
      `);
      
      const [campaignCount] = await connection.execute(`
        SELECT 
          COUNT(*) as total_campaigns,
          COUNT(CASE WHEN status = 2 THEN 1 END) as active_campaigns,
          SUM(daily_budget_eur) as total_daily_budget,
          MAX(updated_at) as last_campaign_update
        FROM gads_campaigns
      `);
      
      const [metricsCount] = await connection.execute(`
        SELECT 
          COUNT(*) as total_metrics,
          MAX(date) as latest_data_date,
          COUNT(DISTINCT google_campaign_id) as campaigns_with_data,
          SUM(cost_eur) as total_cost_tracked
        FROM gads_campaign_metrics
      `);
      
      const [keywordCount] = await connection.execute(`
        SELECT 
          COUNT(*) as total_keywords,
          COUNT(DISTINCT google_campaign_id) as campaigns_with_keywords
        FROM gads_keywords
      `);
      
      res.json({
        success: true,
        sync_history: syncHistory,
        database_status: {
          campaigns: campaignCount[0],
          metrics: metricsCount[0],
          keywords: keywordCount[0]
        },
        recommendations: generateSyncRecommendations(syncHistory, campaignCount[0], metricsCount[0]),
        timestamp: new Date().toISOString()
      });
      
    } finally {
      await connection.end();
    }
    
  } catch (error) {
    console.error('❌ Sync status check failed:', error.message);
    res.status(500).json({
      success: false,
      error: error.message,
      timestamp: new Date().toISOString()
    });
  }
}

/**
 * Generate sync recommendations based on current state
 */
function generateSyncRecommendations(syncHistory, campaignData, metricsData) {
  const recommendations = [];
  
  // Check if any syncs have been run
  if (syncHistory.length === 0) {
    recommendations.push({
      type: 'action',
      title: 'Initial Setup Required',
      message: 'No sync history found. Run campaigns sync first.',
      action: 'POST /gads/google-ads/sync/campaigns'
    });
    return recommendations;
  }
  
  // Check campaign data
  if (campaignData.total_campaigns === 0) {
    recommendations.push({
      type: 'error',
      title: 'No Campaigns Found',
      message: 'Campaign sync completed but no campaigns in database.',
      action: 'Check Google Ads API permissions'
    });
  } else {
    recommendations.push({
      type: 'info',
      title: 'Campaigns Synced',
      message: `${campaignData.total_campaigns} campaigns (${campaignData.active_campaigns} active)`,
      action: null
    });
  }
  
  // Check metrics data
  if (metricsData.total_metrics === 0 && campaignData.active_campaigns > 0) {
    recommendations.push({
      type: 'action',
      title: 'Metrics Sync Needed',
      message: 'Active campaigns found but no metrics data.',
      action: 'POST /gads/google-ads/sync/incremental'
    });
  } else if (metricsData.latest_data_date) {
    const latestDate = new Date(metricsData.latest_data_date);
    const daysSinceLastData = Math.floor((new Date() - latestDate) / (1000 * 60 * 60 * 24));
    
    if (daysSinceLastData > 2) {
      recommendations.push({
        type: 'warning',
        title: 'Metrics Data Stale',
        message: `Latest data is ${daysSinceLastData} days old.`,
        action: 'POST /gads/google-ads/sync/incremental'
      });
    } else {
      recommendations.push({
        type: 'success',
        title: 'Data Up to Date',
        message: `Latest metrics from ${metricsData.latest_data_date}`,
        action: null
      });
    }
  }
  
  // Check for failed syncs
  const recentFailures = syncHistory.filter(s => s.status === 'failed').length;
  if (recentFailures > 0) {
    recommendations.push({
      type: 'error',
      title: 'Recent Sync Failures',
      message: `${recentFailures} failed syncs found. Check error logs.`,
      action: 'Review sync_log error messages'
    });
  }
  
  return recommendations;
}

module.exports = {
  syncGoogleAdsData,
  fullSync,
  incrementalSync,
  backfillSync,
  syncCampaignsOnly,
  syncHistoricalMetricsDateRange,
  // Route handlers
  handleFullSync,
  handleIncrementalSync,
  handleCampaignsSync,
  handleSyncStatus,
  handleDateRangeBackfill
};

/**
 * Handle custom date range backfill route
 */
async function handleDateRangeBackfill(req, res, customer, getDbConnection) {
  try {
    const startDate = req.query.start;
    const endDate = req.query.end;
    
    if (!startDate || !endDate) {
      return res.status(400).json({
        success: false,
        error: 'Both start and end date parameters required (YYYY-MM-DD format)',
        example: '/gads/google-ads/sync/backfill?start=2025-07-01&end=2025-07-31',
        timestamp: new Date().toISOString()
      });
    }
    
    // Validate date formats
    const startDateObj = new Date(startDate);
    const endDateObj = new Date(endDate);
    
    if (isNaN(startDateObj.getTime()) || isNaN(endDateObj.getTime())) {
      return res.status(400).json({
        success: false,
        error: 'Invalid date format. Use YYYY-MM-DD',
        timestamp: new Date().toISOString()
      });
    }
    
    if (startDateObj > endDateObj) {
      return res.status(400).json({
        success: false,
        error: 'Start date must be before end date',
        timestamp: new Date().toISOString()
      });
    }
    
    const daysDiff = Math.ceil((endDateObj - startDateObj) / (1000 * 60 * 60 * 24));
    
    console.log(`🔄 Starting date range backfill (${startDate} to ${endDate}, ${daysDiff} days)...`);
    
    // Return immediately, run sync in background
    res.json({
      success: true,
      service: 'Google Ads Backfill',
      message: `Date range backfill started in background`,
      period: `${startDate} to ${endDate}`,
      days: daysDiff,
      status: 'running',
      timestamp: new Date().toISOString()
    });
    
    // Run backfill in background
    (async () => {
      try {
        const result = await syncHistoricalMetricsDateRange(customer, getDbConnection, {
          startDate,
          endDate,
          description: `${startDate} to ${endDate}`
        });
        console.log(`✅ Date range backfill completed (${startDate} to ${endDate}):`, result);
      } catch (error) {
        console.error(`❌ Date range backfill failed (${startDate} to ${endDate}):`, error.message);
      }
    })();
    
  } catch (error) {
    console.error('❌ Date range backfill failed:', error.message);
    res.status(500).json({
      success: false,
      service: 'Google Ads Backfill',
      error: error.message,
      timestamp: new Date().toISOString()
    });
  }
}

/**
 * Sync historical metrics for specific date range
 */
async function syncHistoricalMetricsDateRange(customer, getDbConnection, options) {
  const { startDate, endDate, description = 'Custom Range' } = options;
  
  let connection;
  
  try {
    console.log(`📊 Backfilling date range: ${startDate} to ${endDate}`);
    
    connection = await getDbConnection();
    
    // Log sync start
    const syncLogId = await logSyncStart(connection, 'backfill', { startDate, endDate });
    
    // Get active campaigns only for backfill
    const activeCampaigns = await getActiveCampaigns(connection);
    
    if (activeCampaigns.length === 0) {
      throw new Error('No active campaigns found for backfill');
    }
    
    console.log(`📊 Backfilling ${activeCampaigns.length} active campaigns for ${description}`);
    
    // Build date range filter for Google Ads API
    const campaignIds = activeCampaigns.map(c => c.google_campaign_id).join(',');
    
    const metricsQuery = `
      SELECT 
        campaign.id,
        segments.date,
        metrics.impressions,
        metrics.clicks,
        metrics.ctr,
        metrics.average_cpc,
        metrics.cost_micros,
        metrics.conversions,
        metrics.view_through_conversions
      FROM campaign
      WHERE campaign.id IN (${campaignIds})
        AND segments.date BETWEEN '${startDate}' AND '${endDate}'
        AND campaign.status = 'ENABLED'
      ORDER BY segments.date DESC, campaign.id
    `;
    
    console.log(`📊 Fetching metrics for ${description}...`);
    const results = await customer.query(metricsQuery);
    
    console.log(`📈 Processing ${results.length} metric records for ${description}...`);
    
    let metricsSynced = 0;
    
    for (const row of results) {
      const metricsData = {
        google_campaign_id: row.campaign.id?.toString(),
        date: row.segments.date,
        impressions: row.metrics?.impressions || 0,
        clicks: row.metrics?.clicks || 0,
        cost_micros: row.metrics?.cost_micros || 0,
        cost_eur: row.metrics?.cost_micros ? (row.metrics.cost_micros / 1000000) : 0,
        conversions: row.metrics?.conversions || 0,
        view_through_conversions: row.metrics?.view_through_conversions || 0,
        ctr: row.metrics?.ctr ? (row.metrics.ctr * 100) : 0,
        cpc_micros: row.metrics?.average_cpc || 0,
        cpc_eur: row.metrics?.average_cpc ? (row.metrics.average_cpc / 1000000) : 0,
        conversion_rate: (row.metrics?.clicks > 0 && row.metrics?.conversions > 0) ? 
          ((row.metrics.conversions / row.metrics.clicks) * 100) : 0,
        cost_per_conversion_eur: (row.metrics?.conversions > 0 && row.metrics?.cost_micros > 0) ? 
          ((row.metrics.cost_micros / 1000000) / row.metrics.conversions) : 0
      };
      
      await upsertCampaignMetrics(connection, metricsData);
      metricsSynced++;
    }
    
    // Log completion
    await logSyncComplete(connection, syncLogId, {
      summary: {
        campaigns_synced: 0,
        metrics_synced: metricsSynced,
        keywords_synced: 0,
        api_calls_used: 1
      }
    });
    
    console.log(`✅ Date range backfill completed (${description}): ${metricsSynced} metrics synced`);
    
    return {
      success: true,
      period: `${startDate} to ${endDate}`,
      metrics_synced: metricsSynced,
      api_calls_used: 1,
      timestamp: new Date().toISOString()
    };
    
  } catch (error) {
    console.error(`❌ Date range backfill failed (${description}):`, error);
    
    if (connection) {
      await logSyncError(connection, error.message);
    }
    
    throw error;
  } finally {
    if (connection) {
      await connection.end();
    }
  }
}