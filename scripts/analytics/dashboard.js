/**
 * Google Ads Dashboard - Real HubSpot Data from MySQL
 * /scripts/analytics/dashboard.js
 * UPDATED: Now displays Campaign ID, Campaign Name, and AdGroup
 */

const GoogleAdsDashboard = () => {
  const [dateRange, setDateRange] = React.useState('7');
  const [isLoading, setIsLoading] = React.useState(true);
  const [dashboardData, setDashboardData] = React.useState(null);
  const [error, setError] = React.useState(null);

  // Fetch dashboard data on load and when date range changes
  React.useEffect(() => {
    fetchDashboardData();
  }, [dateRange]);

  // Fetch data from HubSpot MySQL APIs
  const fetchDashboardData = async () => {
    setIsLoading(true);
    setError(null);
    
    try {
      console.log(`📊 Fetching dashboard data for ${dateRange} days...`);
      
      // Fetch all data in parallel
      const [summaryRes, trendsRes, campaignsRes, territoriesRes] = await Promise.all([
        fetch(`/gads/api/dashboard-data?days=${dateRange}`),
        fetch(`/gads/api/trends?days=${dateRange}`),
        fetch(`/gads/api/campaigns?days=${dateRange}`),
        fetch(`/gads/api/territories?days=${dateRange}`)
      ]);

      const [summaryData, trendsData, campaignsData, territoriesData] = await Promise.all([
        summaryRes.json(),
        trendsRes.json(),
        campaignsRes.json(),
        territoriesRes.json()
      ]);

      // Check for API errors
      if (!summaryData.success) throw new Error(summaryData.error || 'Failed to fetch summary data');
      if (!trendsData.success) throw new Error(trendsData.error || 'Failed to fetch trend data');
      if (!campaignsData.success) throw new Error(campaignsData.error || 'Failed to fetch campaign data');
      if (!territoriesData.success) throw new Error(territoriesData.error || 'Failed to fetch territory data');

      // Combine all data
      const combinedData = {
        summary: summaryData.summary,
        trends: trendsData.trends || [],
        campaigns: campaignsData.campaigns || [],
        territories: territoriesData.territories || [],
        period: summaryData.period
      };

      console.log('✅ Dashboard data loaded:', combinedData);
      setDashboardData(combinedData);

    } catch (err) {
      console.error('❌ Failed to fetch dashboard data:', err);
      setError(err.message);
    }
    
    setIsLoading(false);
  };

  // Format currency
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

  // Handle date range change
  const handleDateRangeChange = (newRange) => {
    setDateRange(newRange);
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
          className: 'loading mb-4',
          key: 'spinner'
        }),
        React.createElement('p', {
          className: 'text-lg text-gray-600',
          key: 'loading-text'
        }, 'Loading dashboard data...'),
        React.createElement('p', {
          className: 'text-sm text-gray-500 mt-2',
          key: 'loading-subtitle'
        }, `Fetching ${dateRange} days of HubSpot data with campaign details`)
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
        React.createElement('div', {
          className: 'text-red-500 text-4xl mb-4',
          key: 'error-icon'
        }, '⚠️'),
        React.createElement('h2', {
          className: 'text-xl font-bold text-gray-900 mb-2',
          key: 'error-title'
        }, 'Dashboard Error'),
        React.createElement('p', {
          className: 'text-gray-600 mb-4',
          key: 'error-message'
        }, error),
        React.createElement('button', {
          className: 'bg-blue-500 hover:bg-blue-600 text-white px-4 py-2 rounded-lg transition-colors',
          onClick: fetchDashboardData,
          key: 'retry-button'
        }, 'Retry Loading')
      ])
    ]);
  }

  // Stat Card Component
  const StatCard = ({ icon, title, value, subtitle, trend }) => (
    React.createElement('div', {
      className: 'bg-white rounded-xl shadow-lg p-6 border border-gray-100 hover:shadow-xl transition-all duration-300 transform hover:-translate-y-1 card-hover'
    }, [
      React.createElement('div', {
        className: 'flex items-center justify-between',
        key: 'card-content'
      }, [
        React.createElement('div', {
          className: 'flex items-center space-x-3',
          key: 'card-main'
        }, [
          React.createElement('div', {
            className: 'p-2 bg-blue-50 rounded-lg',
            key: 'card-icon'
          }, icon),
          React.createElement('div', { key: 'card-text' }, [
            React.createElement('p', {
              className: 'text-sm font-medium text-gray-600',
              key: 'card-title'
            }, title),
            React.createElement('p', {
              className: 'text-2xl font-bold text-gray-900',
              key: 'card-value'
            }, value),
            subtitle && React.createElement('p', {
              className: 'text-xs text-gray-500',
              key: 'card-subtitle'
            }, subtitle)
          ])
        ]),
        trend && React.createElement('div', {
          className: 'text-sm text-green-600',
          key: 'card-trend'
        }, trend)
      ])
    ])
  );

  const { summary, campaigns, territories } = dashboardData;

  return React.createElement('div', {
    className: 'min-h-screen bg-gradient-to-br from-slate-50 to-blue-50 p-6'
  }, [
    // Header
    React.createElement('div', {
      className: 'mb-8',
      key: 'header'
    }, [
      React.createElement('div', {
        className: 'flex flex-col md:flex-row md:items-center md:justify-between',
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
          }, `Real HubSpot data with Campaign ID, Name & AdGroup (${dashboardData.period})`)
        ]),
        
        // Date Range Selector
        React.createElement('div', {
          className: 'mt-4 md:mt-0',
          key: 'date-selector'
        }, [
          React.createElement('div', {
            className: 'flex items-center space-x-2',
            key: 'date-controls'
          }, [
            React.createElement('span', {
              key: 'calendar-icon',
              className: 'text-gray-500'
            }, '📅'),
            React.createElement('select', {
              value: dateRange,
              onChange: (e) => handleDateRangeChange(e.target.value),
              className: 'bg-white border border-gray-300 rounded-lg px-4 py-2 text-gray-900 focus:ring-2 focus:ring-blue-500 focus:border-transparent',
              key: 'date-select'
            }, [
              React.createElement('option', { value: '7', key: '7' }, 'Last 7 days'),
              React.createElement('option', { value: '30', key: '30' }, 'Last 30 days'),
              React.createElement('option', { value: '90', key: '90' }, 'Last 90 days'),
              React.createElement('option', { value: '365', key: '365' }, 'Last year')
            ])
          ])
        ])
      ])
    ]),

    // Key Metrics from real HubSpot data
    React.createElement('div', {
      className: 'grid grid-cols-1 md:grid-cols-2 lg:grid-cols-5 gap-6 mb-8',
      key: 'metrics'
    }, [
      React.createElement(StatCard, {
        icon: '👥',
        title: 'Google Ads Contacts',
        value: formatNumber(summary.totalContacts),
        subtitle: 'From PAID_SEARCH source',
        key: 'contacts-card'
      }),
      // BURN RATE CARD - Fixed link
      React.createElement('div', {
        className: 'bg-red-50 border border-red-200 rounded-xl p-6 cursor-pointer hover:bg-red-100 transition-all duration-200 transform hover:scale-105',
        onClick: () => window.open('/gads/scripts/analytics/burn-rate.html', '_blank'),
        key: 'burn-rate-card'
      }, [
        React.createElement('div', {
          className: 'flex items-center gap-3 mb-3',
          key: 'burn-rate-header'
        }, [
          React.createElement('div', {
            className: 'text-3xl',
            key: 'burn-rate-icon'
          }, '🔥'),
          React.createElement('div', {
            key: 'burn-rate-info'
          }, [
            React.createElement('h3', {
              className: 'font-semibold text-red-800 text-lg',
              key: 'burn-rate-title'
            }, 'Burn Rate'),
            React.createElement('p', {
              className: 'text-sm text-red-600',
              key: 'burn-rate-subtitle'
            }, 'Click to analyze waste')
          ])
        ]),
        React.createElement('div', {
          className: 'text-2xl font-bold text-red-700',
          key: 'burn-rate-value'
        }, territories.find(t => t.isUnsupported) ? 
          `${((territories.find(t => t.isUnsupported).contacts / summary.totalContacts) * 100).toFixed(1)}%` : 
          '0%')
      ]),
      React.createElement(StatCard, {
        icon: '🎯',
        title: 'Deals Created',
        value: formatNumber(summary.totalDeals),
        subtitle: `${summary.conversionRate}% conversion rate`,
        key: 'deals-card'
      }),
      React.createElement(StatCard, {
        icon: '💰',
        title: 'Pipeline Revenue',
        value: formatCurrency(summary.totalRevenue),
        subtitle: `Avg: ${formatCurrency(summary.avgDealValue)}`,
        key: 'revenue-card'
      }),
      React.createElement(StatCard, {
        icon: '📈',
        title: 'Contacts with Deals',
        value: formatNumber(summary.contactsWithDeals),
        subtitle: `${((summary.contactsWithDeals / summary.totalContacts) * 100).toFixed(1)}% have deals`,
        key: 'conversion-card'
      })
    ]),

    // Campaign Performance Table - UPDATED with Campaign ID, Name, AdGroup
    React.createElement('div', {
      className: 'bg-white rounded-xl shadow-lg border border-gray-100 mb-8',
      key: 'campaigns-table'
    }, [
      React.createElement('div', {
        className: 'p-6 border-b border-gray-200',
        key: 'table-header'
      }, [
        React.createElement('h3', {
          className: 'text-lg font-semibold text-gray-900',
          key: 'table-title'
        }, `Campaign Performance (${campaigns.length} campaigns)`),
        React.createElement('p', {
          className: 'text-sm text-gray-500 mt-1',
          key: 'table-subtitle'
        }, 'Now showing Campaign ID, Campaign Name & AdGroup from HubSpot')
      ]),
      
      campaigns.length > 0 ? React.createElement('div', {
        className: 'overflow-x-auto',
        key: 'table-container'
      }, [
        React.createElement('table', {
          className: 'min-w-full divide-y divide-gray-200',
          key: 'campaigns-table-element'
        }, [
          React.createElement('thead', {
            className: 'bg-gray-50',
            key: 'table-head'
          }, [
            React.createElement('tr', { key: 'header-row' }, [
              React.createElement('th', {
                className: 'px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider',
                key: 'campaign-name-header'
              }, 'Campaign Name'),
              React.createElement('th', {
                className: 'px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider',
                key: 'campaign-id-header'
              }, 'Campaign ID'),
              React.createElement('th', {
                className: 'px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider',
                key: 'adgroup-header'
              }, 'AdGroup'),
              React.createElement('th', {
                className: 'px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider',
                key: 'contacts-header'
              }, 'Contacts'),
              React.createElement('th', {
                className: 'px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider',
                key: 'deals-header'
              }, 'Deals'),
              React.createElement('th', {
                className: 'px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider',
                key: 'conversion-header'
              }, 'SQL Rate'),
              React.createElement('th', {
                className: 'px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider',
                key: 'revenue-header'
              }, 'Revenue'),
            ])
          ]),
          React.createElement('tbody', {
            className: 'bg-white divide-y divide-gray-200',
            key: 'table-body'
          }, campaigns.map((campaign, index) =>
            React.createElement('tr', {
              className: 'hover:bg-gray-50 transition-colors duration-200',
              key: `campaign-${index}`
            }, [
              // Campaign Name (google_campaign_name)
              React.createElement('td', {
                className: 'px-6 py-4 text-sm font-medium text-gray-900',
                key: 'campaign-name'
              }, [
                React.createElement('div', {
                  className: 'font-bold text-blue-600',
                  key: 'name-main'
                }, campaign.googleCampaignName || campaign.name || 'Unknown Campaign'),
                React.createElement('div', {
                  className: 'text-xs text-gray-400 mt-1',
                  key: 'name-source'
                }, 'google_campaign_name')
              ]),
              
              // Campaign ID (google_campaign_id or fallback)
              React.createElement('td', {
                className: 'px-6 py-4 whitespace-nowrap text-sm text-gray-500 font-mono',
                key: 'campaign-id',
                title: 'Google Ads Campaign ID'
              }, [
                React.createElement('div', {
                  className: 'bg-gray-100 px-2 py-1 rounded text-xs',
                  key: 'id-display'
                }, campaign.googleCampaignId || campaign.campaignId || 'N/A'),
                React.createElement('div', {
                  className: 'text-xs text-gray-400 mt-1',
                  key: 'id-source'
                }, 'google_campaign_id')
              ]),
              
              // AdGroup (adgroup)
              React.createElement('td', {
                className: 'px-6 py-4 whitespace-nowrap text-sm text-gray-500',
                key: 'adgroup'
              }, [
                React.createElement('div', {
                  className: 'text-sm text-gray-700',
                  key: 'adgroup-main'
                }, campaign.adgroup || 'Not specified'),
                React.createElement('div', {
                  className: 'text-xs text-gray-400 mt-1',
                  key: 'adgroup-source'
                }, 'adgroup')
              ]),
              
              React.createElement('td', {
                className: 'px-6 py-4 whitespace-nowrap text-sm text-gray-500',
                key: 'contacts'
              }, formatNumber(campaign.contacts)),
              React.createElement('td', {
                className: 'px-6 py-4 whitespace-nowrap text-sm text-gray-500',
                key: 'deals'
              }, formatNumber(campaign.deals)),
              React.createElement('td', {
                className: 'px-6 py-4 whitespace-nowrap text-sm font-medium',
                key: 'conversion'
              }, React.createElement('span', {
                className: `inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${
                  parseFloat(campaign.conversionRate) > 20 ? 'bg-green-100 text-green-800' :
                  parseFloat(campaign.conversionRate) > 10 ? 'bg-yellow-100 text-yellow-800' :
                  'bg-red-100 text-red-800'
                }`
              }, `${campaign.conversionRate}%`)),
              React.createElement('td', {
                className: 'px-6 py-4 whitespace-nowrap text-sm text-gray-500',
                key: 'revenue'
              }, formatCurrency(campaign.revenue))
            ])
          ))
        ])
      ]) : React.createElement('div', {
        className: 'p-8 text-center',
        key: 'no-campaigns'
      }, [
        React.createElement('p', {
          className: 'text-gray-500',
          key: 'no-campaigns-text'
        }, 'No campaign data available for this period')
      ])
    ]),

    // Territory Performance (Real HubSpot Data)
    React.createElement('div', {
      className: 'bg-white rounded-xl shadow-lg border border-gray-100 mb-8',
      key: 'territories-section'
    }, [
      React.createElement('div', {
        className: 'p-6 border-b border-gray-200',
        key: 'territories-header'
      }, [
        React.createElement('h3', {
          className: 'text-lg font-semibold text-gray-900',
          key: 'territories-title'
        }, `Territory Performance (${territories.length} countries)`),
        React.createElement('p', {
          className: 'text-sm text-gray-500 mt-1',
          key: 'territories-subtitle'
        }, 'Based on country and ip_country fields')
      ]),
      
      territories.length > 0 ? React.createElement('div', {
        className: 'p-6',
        key: 'territories-content'
      }, [
        React.createElement('div', {
          className: 'grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4',
          key: 'territories-grid'
        }, territories.slice(0, 9).map((territory, index) =>
          React.createElement('div', {
            className: `bg-gray-50 rounded-lg p-4 hover:bg-gray-100 transition-colors ${
              territory.name === 'Unsupported Territory' ? 'cursor-pointer hover:bg-red-50 border border-red-200' : ''
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
              }, `🎯 ${formatNumber(territory.deals)} deals`),
              React.createElement('div', {
                key: 'revenue-stat'
              }, `💰 ${formatCurrency(territory.revenue)}`),
              React.createElement('div', {
                key: 'conversion-stat'
              }, `📊 ${territory.conversionRate}% conversion`),
              // Fixed burn rate indicator - now clickable and properly linked
              territory.name === 'Unsupported Territory' ? 
                React.createElement('div', {
                  key: 'burn-rate-indicator',
                  className: 'text-red-600 font-medium flex items-center gap-1'
                }, [
                  React.createElement('span', { key: 'fire-icon' }, '🔥'),
                  React.createElement('span', { key: 'burn-text' }, 'Click to analyze burn rate')
                ]) : null
            ])
          ])
        ))
      ]) : React.createElement('div', {
        className: 'p-8 text-center',
        key: 'no-territories'
      }, [
        React.createElement('p', {
          className: 'text-gray-500',
          key: 'no-territories-text'
        }, 'No territory data available for this period')
      ])
    ]),

    // Data Source Info - UPDATED
    React.createElement('div', {
      className: 'bg-blue-50 border border-blue-200 rounded-lg p-4 mb-8',
      key: 'data-info'
    }, [
      React.createElement('div', {
        className: 'flex items-center space-x-2',
        key: 'data-info-content'
      }, [
        React.createElement('span', {
          key: 'info-icon',
          className: 'text-blue-500'
        }, 'ℹ️'),
        React.createElement('div', {
          key: 'info-text'
        }, [
          React.createElement('p', {
            className: 'text-sm font-medium text-blue-900',
            key: 'info-title'
          }, 'Data Source: HubSpot MySQL Database with Campaign Details'),
          React.createElement('p', {
            className: 'text-xs text-blue-700',
            key: 'info-details'
          }, `PAID_SEARCH contacts with google_campaign_id, google_campaign_name & adgroup | Last sync: ${new Date().toLocaleString()} | ${summary.totalContacts} total contacts found`)
        ])
      ])
    ]),

    // Footer
    React.createElement('div', {
      className: 'mt-8 text-center text-sm text-gray-500',
      key: 'footer'
    }, [
      React.createElement('p', {
        key: 'footer-text'
      }, `Dashboard updated: ${new Date().toLocaleString()} | Enhanced with Campaign ID, Name & AdGroup data`)
    ])
  ]);
};