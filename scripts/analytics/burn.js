/**
 * Google Ads Budget Module
 * Path: /home/hub/public_html/gads/scripts/google/budget.js
 * 
 * Handles all Google Ads budget-related queries and analysis
 * Receives authenticated Google Ads client from index.js
 */

/**
 * Get budget information for all campaigns
 * @param {Object} customer - Authenticated Google Ads customer client from index.js
 * @returns {Object} Budget information
 */
async function getBudgetInfo(customer) {
  try {
    console.log('💰 Fetching Google Ads budget information...');
    
    if (!customer) {
      throw new Error('Google Ads customer client is required');
    }
    
    // Query budget information with more details
    const query = `
      SELECT 
        campaign_budget.id,
        campaign_budget.name,
        campaign_budget.amount_micros,
        campaign_budget.delivery_method,
        campaign_budget.explicitly_shared,
        campaign_budget.status,
        campaign_budget.type
      FROM campaign_budget
      WHERE campaign_budget.status != 'REMOVED'
      ORDER BY campaign_budget.amount_micros DESC
    `;
    
    console.log('   💰 Executing budget query...');
    const results = await customer.query(query);
    
    console.log(`   ✅ Found ${results.length} budgets`);
    
    // Process budget results
    const budgets = results.map(row => {
      const budget = row.campaign_budget;
      const dailyAmountEur = budget.amount_micros ? (budget.amount_micros / 1000000).toFixed(2) : '0.00';
      const monthlyAmountEur = (parseFloat(dailyAmountEur) * 30.44).toFixed(2); // Average days per month
      const yearlyAmountEur = (parseFloat(dailyAmountEur) * 365).toFixed(2);
      
      return {
        id: budget.id?.toString(),
        name: budget.name,
        daily_amount_micros: budget.amount_micros || 0,
        daily_amount: dailyAmountEur,
        monthly_amount: monthlyAmountEur,
        yearly_amount: yearlyAmountEur,
        delivery_method: budget.delivery_method,
        shared: budget.explicitly_shared || false,
        status: budget.status,
        type: budget.type
      };
    });
    
    // Calculate summary statistics
    const summary = {
      total_budgets: budgets.length,
      shared_budgets: budgets.filter(b => b.shared).length,
      standard_budgets: budgets.filter(b => !b.shared).length,
      total_daily_spend: budgets.reduce((sum, b) => sum + parseFloat(b.daily_amount), 0).toFixed(2),
      total_monthly_spend: budgets.reduce((sum, b) => sum + parseFloat(b.monthly_amount), 0).toFixed(2),
      total_yearly_spend: budgets.reduce((sum, b) => sum + parseFloat(b.yearly_amount), 0).toFixed(2),
      average_daily_budget: budgets.length > 0 ? 
        (budgets.reduce((sum, b) => sum + parseFloat(b.daily_amount), 0) / budgets.length).toFixed(2) : '0.00',
      highest_daily_budget: budgets.length > 0 ? 
        Math.max(...budgets.map(b => parseFloat(b.daily_amount))).toFixed(2) : '0.00',
      lowest_daily_budget: budgets.length > 0 ? 
        Math.min(...budgets.map(b => parseFloat(b.daily_amount))).toFixed(2) : '0.00'
    };
    
    console.log(`   ✅ Budget analysis complete:`);
    console.log(`      💰 Total budgets: ${summary.total_budgets}`);
    console.log(`      📊 Total daily spend: €${summary.total_daily_spend}`);
    console.log(`      📅 Total monthly spend: €${summary.total_monthly_spend}`);
    console.log(`      📈 Total yearly spend: €${summary.total_yearly_spend}`);
    
    return {
      success: true,
      count: budgets.length,
      summary,
      budgets,
      timestamp: new Date().toISOString()
    };
    
  } catch (error) {
    console.error('❌ Budget information fetch failed:', error.message);
    return {
      success: false,
      error: error.message,
      timestamp: new Date().toISOString()
    };
  }
}

/**
 * Get budget utilization and spending trends
 * @param {Object} customer - Authenticated Google Ads customer client
 * @param {Object} options - Query options
 * @param {number} options.days - Number of days to analyze (default: 30)
 * @returns {Object} Budget utilization data
 */
async function getBudgetUtilization(customer, options = {}) {
  try {
    console.log('📊 Analyzing budget utilization...');
    
    if (!customer) {
      throw new Error('Google Ads customer client is required');
    }
    
    const days = options.days || 30;
    
    // Query campaign performance vs budget
    const query = `
      SELECT 
        campaign.id,
        campaign.name,
        campaign_budget.id,
        campaign_budget.name,
        campaign_budget.amount_micros,
        segments.date,
        metrics.cost_micros
      FROM campaign
      WHERE segments.date DURING LAST_${days}_DAYS
        AND campaign.status != 'REMOVED'
        AND campaign_budget.status != 'REMOVED'
      ORDER BY segments.date DESC, metrics.cost_micros DESC
    `;
    
    console.log(`   📊 Analyzing budget utilization for last ${days} days...`);
    const results = await customer.query(query);
    
    console.log(`   ✅ Found ${results.length} campaign-day records`);
    
    // Process results by budget
    const budgetUtilization = new Map();
    
    results.forEach(row => {
      const budgetId = row.campaign_budget.id?.toString();
      
      if (!budgetUtilization.has(budgetId)) {
        budgetUtilization.set(budgetId, {
          budget_id: budgetId,
          budget_name: row.campaign_budget.name,
          daily_budget_micros: row.campaign_budget.amount_micros || 0,
          daily_budget: row.campaign_budget.amount_micros ? 
            (row.campaign_budget.amount_micros / 1000000).toFixed(2) : '0.00',
          campaigns: new Set(),
          daily_spend: [],
          total_spend_micros: 0,
          days_analyzed: 0
        });
      }
      
      const budget = budgetUtilization.get(budgetId);
      budget.campaigns.add(`${row.campaign.name} (${row.campaign.id})`);
      
      const dailySpend = row.metrics?.cost_micros || 0;
      budget.daily_spend.push({
        date: row.segments.date,
        spend_micros: dailySpend,
        spend: (dailySpend / 1000000).toFixed(2),
        utilization_percent: budget.daily_budget_micros > 0 ? 
          ((dailySpend / budget.daily_budget_micros) * 100).toFixed(2) : '0.00'
      });
      
      budget.total_spend_micros += dailySpend;
    });
    
    // Calculate utilization metrics
    const utilization = Array.from(budgetUtilization.values()).map(budget => {
      const uniqueDays = [...new Set(budget.daily_spend.map(d => d.date))].length;
      const totalSpend = (budget.total_spend_micros / 1000000).toFixed(2);
      const potentialSpend = (parseFloat(budget.daily_budget) * uniqueDays).toFixed(2);
      const utilizationPercent = potentialSpend > 0 ? 
        ((parseFloat(totalSpend) / parseFloat(potentialSpend)) * 100).toFixed(2) : '0.00';
      
      return {
        budget_id: budget.budget_id,
        budget_name: budget.budget_name,
        daily_budget: budget.daily_budget,
        campaigns_count: budget.campaigns.size,
        campaigns: Array.from(budget.campaigns),
        days_analyzed: uniqueDays,
        total_spend: totalSpend,
        potential_spend: potentialSpend,
        utilization_percent: utilizationPercent,
        average_daily_spend: uniqueDays > 0 ? 
          (parseFloat(totalSpend) / uniqueDays).toFixed(2) : '0.00',
        underutilized: parseFloat(utilizationPercent) < 80,
        over_budget: parseFloat(utilizationPercent) > 100,
        daily_breakdown: budget.daily_spend.sort((a, b) => a.date.localeCompare(b.date))
      };
    });
    
    // Sort by utilization percentage (lowest first to identify issues)
    utilization.sort((a, b) => parseFloat(a.utilization_percent) - parseFloat(b.utilization_percent));
    
    // Calculate overall summary
    const summary = {
      budgets_analyzed: utilization.length,
      underutilized_budgets: utilization.filter(u => u.underutilized).length,
      over_budget_campaigns: utilization.filter(u => u.over_budget).length,
      total_potential_spend: utilization.reduce((sum, u) => sum + parseFloat(u.potential_spend), 0).toFixed(2),
      total_actual_spend: utilization.reduce((sum, u) => sum + parseFloat(u.total_spend), 0).toFixed(2),
      overall_utilization: utilization.length > 0 ? 
        ((utilization.reduce((sum, u) => sum + parseFloat(u.utilization_percent), 0) / utilization.length)).toFixed(2) : '0.00',
      days_analyzed: days
    };
    
    console.log(`   ✅ Budget utilization analysis complete:`);
    console.log(`      📊 ${summary.budgets_analyzed} budgets analyzed`);
    console.log(`      📉 ${summary.underutilized_budgets} underutilized budgets`);
    console.log(`      📈 ${summary.over_budget_campaigns} over-budget campaigns`);
    console.log(`      💰 Overall utilization: ${summary.overall_utilization}%`);
    
    return {
      success: true,
      summary,
      utilization,
      period: `Last ${days} days`,
      timestamp: new Date().toISOString()
    };
    
  } catch (error) {
    console.error('❌ Budget utilization analysis failed:', error.message);
    return {
      success: false,
      error: error.message,
      timestamp: new Date().toISOString()
    };
  }
}

// Export functions
module.exports = {
  getBudgetInfo,
  getBudgetUtilization
};