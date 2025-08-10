/**
 * Google Ads Dashboard - FIXED VERSION
 * /scripts/analytics/dashboard.js
 * 
 * FIXES:
 * - Removed syntax errors
 * - Fixed API endpoint calls
 * - Simplified for compatibility
 * - Enhanced error handling
 */

const GoogleAdsDashboard = () => {
  const [dateRange, setDateRange] = React.useState('7');
  const [analysisMode, setAnalysisMode] = React.useState('pipeline');
  const [selectedCampaign, setSelectedCampaign] = React.useState('all');
  const [isLoading, setIsLoading] = React.useState(true);
  const [dashboardData, setDashboardData] = React.useState(null);
  const [error, setError] = React.useState(null);

  // Fetch dashboard data when parameters change
  React.useEffect(() => {
    fetchDashboardData();
  }, [dateRange, analysisMode, selectedCampaign]);

  // Fetch data from APIs
  const fetchDashboardData = async () => {
    setIsLoading(true);
    setError(null);
    
    try {
      console.log(`📊 Fetching dashboard data: ${dateRange} days, ${analysisMode} mode`);
      
      // Build API URLs
      const baseParams = `days=${dateRange}&mode=${analysisMode}`;
      
      // Fetch data in parallel
      const [summaryRes, campaignsRes, territoriesRes] = await Promise.all([
        fetch(`/gads/analytics/dashboard-data?${baseParams}`),
        fetch(`/gads/analytics/campaigns?${baseParams}`),
        fetch(`/gads/analytics/territories?${baseParams}`)
      ]);

      // Check for errors
      if (!summaryRes.ok) throw new Error(`Summary API failed: ${summaryRes.status}`);
      if (!campaignsRes.ok) throw new Error(`Campaigns API failed: ${campaignsRes.status}`);
      if (!territoriesRes.ok) throw new Error(`Territories API failed: ${territoriesRes.status}`);

      const [summaryData, campaignsData, territoriesData] = await Promise.all([
        summaryRes.json(),
        campaignsRes.json(),
        territoriesRes.json()
      ]);

      // Check API success
      if (!summaryData.success) throw new Error(summaryData.error || 'Summary failed');
      if (!campaignsData.success) throw new Error(campaignsData.error || 'Campaigns failed');
      if (!territoriesData.success) throw new Error(territoriesData.error || 'Territories failed');

      // Combine data
      const combinedData = {
        summary: summaryData.summary,
        campaigns: campaignsData.campaigns || [],
        territories: territoriesData.territories || [],
        mqlValidation: summaryData.mql_validation_details,
        period: summaryData.period,
        analysisMode: analysisMode
      };

      console.log('✅ Dashboard data loaded:', combinedData);
      setDashboardData(combinedData);

    } catch (err) {
      console.error('❌ Dashboard data fetch failed:', err);
      setError(err.message);
    }
    
    setIsLoading(false);
  };

  // Format functions
  const formatCurrency = (value) => {
    return new Intl.NumberFormat('en-US', {
      style: 'currency',
      currency: 'EUR',
      minimumFractionDigits: 0,
      maximumFractionDigits: 0,
    }).format(value || 0);
  };

  const formatNumber = (value) => {
    return new Intl.NumberFormat().format(value || 0);
  };

  // Event handlers
  const handleDateRangeChange = (newRange) => {
    setDateRange(newRange);
  };

  const handleAnalysisModeChange = (newMode) => {
    setAnalysisMode(newMode);
  };

  const handleCampaignChange = (newCampaign) => {
    setSelectedCampaign(newCampaign);
  };

  // Loading state
  if (isLoading || !dashboardData) {
    return React.createElement('div', {
      className: 'min-h-screen bg-gradient-to-br from-slate-50 to-blue-50 flex items-center justify-center'
    }, [
      React.createElement('div', {
        className: 'text-center',
        key: 'loading'
      }, [
        React.createElement('div', {
          className: 'text-4xl mb-4',
          key: 'spinner'
        }, '🔄'),
        React.createElement('p', {
          className: 'text-lg text-gray-600',
          key: 'loading-text'
        }, 'Loading dashboard data...'),
        React.createElement('p', {
          className: 'text-sm text-gray-500 mt-2',
          key: 'loading-subtitle'
        }, `${dateRange} days (${analysisMode} mode)`)
      ])
    ]);
  }

  // Error state
  if (error) {
    return React.createElement('div', {
      className: 'min-h-screen bg-gradient-to-br from-slate-50 to-blue-50 flex items-center justify-center'
    }, [
      React.createElement('div', {
        className: 'text-center bg-white p-8 rounded-xl shadow-lg max-w-md',
        key: 'error'
      }, [
        React.createElement('h2', {
          className: 'text-xl font-bold text-red-600 mb-4',
          key: 'error-title'
        }, '❌ Dashboard Error'),
        React.createElement('p', {
          className: 'text-gray-600 mb-4',
          key: 'error-message'
        }, error),
        React.createElement('button', {
          className: 'bg-blue-500 text-white px-4 py-2 rounded hover:bg-blue-600',
          onClick: () => window.location.reload(),
          key: 'reload-btn'
        }, 'Reload Dashboard')
      ])
    ]);
  }

  // Helper function to create metric cards
  const createMetricCard = (title, value, trend, icon, color = 'blue') => {
    return React.createElement('div', {
      className: 'bg-white rounded-lg shadow p-6',
      key: `card-${title.replace(/\s+/g, '-').toLowerCase()}`
    }, [
      React.createElement('div', {
        className: 'flex items-center justify-between',
        key: 'card-header'
      }, [
        React.createElement('div', { key: 'card-content' }, [
          React.createElement('p', {
            className: 'text-sm font-medium text-gray-600',
            key: 'card-title'
          }, title),
          React.createElement('p', {
            className: `text-2xl font-bold text-${color}-600`,
            key: 'card-value'
          }, value)
        ]),
        React.createElement('div', {
          className: `text-${color}-600`,
          key: 'card-icon'
        }, icon)
      ]),
      trend && React.createElement('div', {
        className: 'text-sm text-green-600 mt-2',
        key: 'card-trend'
      }, trend)
    ]);
  };

  const { summary, campaigns, territories, mqlValidation } = dashboardData;

  return React.createElement('div', {
    className: 'min-h-screen bg-gradient-to-br from-slate-50 to-blue-50 p-6'
  }, [
    // Header
    React.createElement('div', {
      className: 'mb-8',
      key: 'header'
    }, [
      React.createElement('div', {
        className: 'flex flex-col lg:flex-row lg:items-center lg:justify-between',
        key: 'header-content'
      }, [
        React.createElement('div', { key: 'header-text' }, [
          React.createElement('h1', {
            className: 'text-3xl font-bold text-gray-900 mb-2',
            key: 'title'
          }, 'Google Ads Pipeline Dashboard'),
          React.createElement('p', {
            className: 'text-gray-600',
            key: 'subtitle'
          }, `Real HubSpot data from MySQL (${dashboardData.period})`)
        ]),
        
        // Controls Panel
        React.createElement('div', {
          className: 'mt-4 lg:mt-0 grid grid-cols-1 md:grid-cols-3 gap-4',
          key: 'controls-panel'
        }, [
          // Date Range
          React.createElement('div', {
            className: 'flex flex-col',
            key: 'date-selector'
          }, [
            React.createElement('label', {
              className: 'text-sm font-medium text-gray-700 mb-1',
              key: 'date-label'
            }, 'Date Range'),
            React.createElement('select', {
              value: dateRange,
              onChange: (e) => handleDateRangeChange(e.target.value),
              className: 'bg-white border border-gray-300 rounded-lg px-3 py-2 text-gray-900 focus:ring-2 focus:ring-blue-500',
              key: 'date-select'
            }, [
              React.createElement('option', { value: '7', key: '7' }, 'Last 7 days'),
              React.createElement('option', { value: '14', key: '14' }, 'Last 14 days'),
              React.createElement('option', { value: '30', key: '30' }, 'Last 30 days'),
              React.createElement('option', { value: '60', key: '60' }, 'Last 60 days'),
              React.createElement('option', { value: '90', key: '90' }, 'Last 90 days')
            ])
          ]),

          // Analysis Mode
          React.createElement('div', {
            className: 'flex flex-col',
            key: 'analysis-mode-selector'
          }, [
            React.createElement('label', {
              className: 'text-sm font-medium text-gray-700 mb-1',
              key: 'mode-label'
            }, 'Analysis Mode'),
            React.createElement('select', {
              value: analysisMode,
              onChange: (e) => handleAnalysisModeChange(e.target.value),
              className: 'bg-white border border-gray-300 rounded-lg px-3 py-2 text-gray-900 focus:ring-2 focus:ring-blue-500',
              key: 'mode-select'
            }, [
              React.createElement('option', { value: 'pipeline', key: 'pipeline' }, 'Pipeline Analysis'),
              React.createElement('option', { value: 'revenue', key: 'revenue' }, 'Revenue Analysis')
            ])
          ]),

          // Campaign Filter
          React.createElement('div', {
            className: 'flex flex-col',
            key: 'campaign-selector'
          }, [
            React.createElement('label', {
              className: 'text-sm font-medium text-gray-700 mb-1',
              key: 'campaign-label'
            }, 'Campaign Filter'),
            React.createElement('select', {
              value: selectedCampaign,
              onChange: (e) => handleCampaignChange(e.target.value),
              className: 'bg-white border border-gray-300 rounded-lg px-3 py-2 text-gray-900 focus:ring-2 focus:ring-blue-500',
              key: 'campaign-select'
            }, [
              React.createElement('option', { value: 'all', key: 'all' }, 'All Campaigns'),
              ...campaigns.slice(0, 10).map((campaign, index) => 
                React.createElement('option', { 
                  value: campaign.name, 
                  key: `campaign-${index}` 
                }, campaign.name)
              )
            ])
          ])
        ])
      ]),

      // Mode Explanation
      React.createElement('div', {
        className: 'mt-4 p-4 bg-blue-50 rounded-lg border border-blue-200',
        key: 'mode-explanation'
      }, [
        React.createElement('div', {
          className: 'flex items-start space-x-2',
          key: 'explanation-content'
        }, [
          React.createElement('div', {
            className: 'text-blue-600',
            key: 'info-icon'
          }, 'ℹ️'),
          React.createElement('div', {
            className: 'text-sm text-blue-800',
            key: 'explanation-text'
          }, [
            React.createElement('strong', { key: 'mode-title' }, 
              analysisMode === 'pipeline' ? 'Pipeline Analysis Mode' : 'Revenue Analysis Mode'
            ),
            React.createElement('span', { key: 'mode-desc' }, 
              analysisMode === 'pipeline' 
                ? ' - Shows deals created in date range (up-to-the-minute pipeline data)'
                : ' - Shows deals closed in date range (revenue focus, excludes active pipeline)'
            )
          ])
        ])
      ])
    ]),

    // MQL Validation Section
    mqlValidation && mqlValidation.success && React.createElement('div', {
      className: 'mb-8',
      key: 'mql-validation-section'
    }, [
      React.createElement('h2', {
        className: 'text-xl font-bold text-gray-900 mb-4',
        key: 'mql-title'
      }, '🎯 MQL → SQL Validation Pipeline'),
      
      React.createElement('div', {
        className: 'grid grid-cols-1 md:grid-cols-4 gap-6',
        key: 'mql-metrics'
      }, [
        createMetricCard(
          'Total MQLs',
          formatNumber(mqlValidation.mql_stage.total_mqls),
          null,
          '🎯',
          'blue'
        ),
        createMetricCard(
          'Territory Validation',
          `${formatNumber(mqlValidation.mql_stage.supported_mqls)} passed`,
          `${mqlValidation.mql_stage.burn_rate_percentage}% burn rate`,
          '🌍',
          'green'
        ),
        createMetricCard(
          'SQLs Created',
          formatNumber(mqlValidation.sql_validation.total_deals_created),
          `${mqlValidation.sql_validation.validation_rate_percentage}% conversion`,
          '📋',
          'purple'
        ),
        createMetricCard(
          'Won Deals',
          formatNumber(mqlValidation.sql_validation.won_deals),
          mqlValidation.sql_validation.lost_deals > 0 ? `${mqlValidation.sql_validation.lost_deals} lost` : null,
          '🏆',
          'green'
        )
      ])
    ]),

    // Key Metrics Summary
    React.createElement('div', {
      className: 'grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-8',
      key: 'metrics-grid'
    }, [
      createMetricCard(
        'Total Deals',
        formatNumber(summary.total_deals),
        summary.active_deals > 0 ? `${summary.active_deals} active` : null,
        '📊'
      ),
      createMetricCard(
        'Won Deals',
        formatNumber(summary.won_deals),
        summary.lost_deals > 0 ? `${summary.lost_deals} lost` : null,
        '🏆',
        'green'
      ),
      createMetricCard(
        'Total Revenue',
        formatCurrency(summary.total_value),
        summary.avg_deal_size > 0 ? `Avg: ${formatCurrency(summary.avg_deal_size)}` : null,
        '💰',
        'green'
      ),
      createMetricCard(
        'Conversion Rate',
        `${summary.conversion_rate}%`,
        null,
        '📈',
        'purple'
      )
    ]),

    // Territory and Campaign Analysis
    React.createElement('div', {
      className: 'grid grid-cols-1 lg:grid-cols-2 gap-8 mb-8',
      key: 'analysis-section'
    }, [
      // Territory Performance
      React.createElement('div', {
        className: 'bg-white rounded-lg shadow p-6',
        key: 'territory-performance'
      }, [
        React.createElement('h3', {
          className: 'text-lg font-semibold text-gray-900 mb-4',
          key: 'territory-title'
        }, '🌍 Territory Performance'),
        
        React.createElement('div', {
          className: 'space-y-4',
          key: 'territory-list'
        }, territories.slice(0, 8).map((territory, index) => 
          React.createElement('div', {
            className: `p-4 rounded-lg border ${
              territory.name === 'Unsupported Territory' ? 
              'cursor-pointer hover:bg-red-50 border border-red-200 bg-red-25' : 
              'border-gray-200'
            }`,
            onClick: territory.name === 'Unsupported Territory' ? 
              () => window.open('/gads/scripts/analytics/burn-rate.html', '_blank') : 
              undefined,
            key: `territory-${index}`
          }, [
            React.createElement('div', {
              className: 'flex items-center justify-between mb-2',
              key: 'territory-header'
            }, [
              React.createElement('h4', {
                className: 'font-medium text-gray-900',
                key: 'territory-name'
              }, territory.name),
              React.createElement('div', {
                className: 'w-3 h-3 rounded-full',
                style: { backgroundColor: territory.color },
                key: 'territory-color'
              })
            ]),
            React.createElement('div', {
              className: 'space-y-1 text-sm text-gray-600',
              key: 'territory-stats'
            }, [
              React.createElement('div', {
                key: 'contacts-stat'
              }, `👥 ${formatNumber(territory.contacts)} contacts`),
              React.createElement('div', {
                key: 'deals-stat'
              }, `🎯 ${formatNumber(territory.deals || territory.deals_created)} deals`),
              React.createElement('div', {
                key: 'revenue-stat'
              }, `💰 ${formatCurrency(territory.revenue)}`),
              React.createElement('div', {
                key: 'conversion-stat'
              }, `📊 ${territory.mql_to_sql_rate || territory.conversion_rate}% conversion`),
              territory.name === 'Unsupported Territory' && React.createElement('div', {
                className: 'text-red-600 font-medium',
                key: 'burn-indicator'
              }, '🔥 BURN RATE ALERT - Click for details')
            ])
          ])
        ))
      ]),

      // Campaign Performance
      React.createElement('div', {
        className: 'bg-white rounded-lg shadow p-6',
        key: 'campaign-performance'
      }, [
        React.createElement('h3', {
          className: 'text-lg font-semibold text-gray-900 mb-4',
          key: 'campaign-title'
        }, '🎯 Campaign Performance'),
        
        React.createElement('div', {
          className: 'space-y-4',
          key: 'campaign-list'
        }, campaigns.slice(0, 8).map((campaign, index) => 
          React.createElement('div', {
            className: 'p-4 border border-gray-200 rounded-lg',
            key: `campaign-${index}`
          }, [
            React.createElement('div', {
              className: 'flex items-center justify-between mb-2',
              key: 'campaign-header'
            }, [
              React.createElement('h4', {
                className: 'font-medium text-gray-900 truncate',
                key: 'campaign-name'
              }, campaign.name),
              React.createElement('span', {
                className: 'text-sm text-green-600 font-medium',
                key: 'campaign-won'
              }, `${campaign.won_deals} won`)
            ]),
            React.createElement('div', {
              className: 'grid grid-cols-2 gap-4 text-sm text-gray-600',
              key: 'campaign-stats'
            }, [
              React.createElement('div', { key: 'contacts' }, `👥 ${formatNumber(campaign.contacts)} contacts`),
              React.createElement('div', { key: 'deals' }, `🎯 ${formatNumber(campaign.deals)} deals`),
              React.createElement('div', { key: 'revenue' }, `💰 ${formatCurrency(campaign.revenue)}`),
              React.createElement('div', { key: 'rate' }, `📊 ${campaign.mql_to_sql_rate || campaign.conversion_rate}%`)
            ])
          ])
        ))
      ])
    ]),

    // Footer
    React.createElement('div', {
      className: 'mt-8 text-center text-sm text-gray-500',
      key: 'footer'
    }, [
      React.createElement('p', { key: 'footer-text' }, 
        `Dashboard updated: ${new Date().toLocaleString()} | ` +
        `Mode: ${analysisMode === 'pipeline' ? 'Pipeline Analysis' : 'Revenue Analysis'} | ` +
        `Campaign: ${selectedCampaign === 'all' ? 'All Campaigns' : selectedCampaign}`
      ),
      React.createElement('p', { key: 'footer-note', className: 'mt-2' }, 
        'Data source: Real HubSpot CRM data synchronized to MySQL | Enhanced with schema corrections'
      )
    ])
  ]);
};

// Render the dashboard
const container = document.getElementById('dashboard-root');
const root = ReactDOM.createRoot(container);
root.render(React.createElement(GoogleAdsDashboard));