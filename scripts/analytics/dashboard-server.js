/**
 * Dashboard Server - HTML Template & React Setup
 * /scripts/analytics/dashboard-server.js
 */

const fs = require('fs');
const path = require('path');

/**
 * Serve the React dashboard with all required dependencies
 */
function serveDashboard(req, res) {
  try {
    // Read the React component
    const dashboardComponentPath = path.join(__dirname, 'dashboard.js');
    const dashboardComponent = fs.readFileSync(dashboardComponentPath, 'utf8');
    
    // Generate the complete HTML page
    const html = `
      <!DOCTYPE html>
      <html lang="en">
      <head>
          <meta charset="UTF-8">
          <meta name="viewport" content="width=device-width, initial-scale=1.0">
          <title>Google Ads Pipeline Dashboard</title>
          
          <!-- React & Dependencies -->
          <script crossorigin src="https://unpkg.com/react@18/umd/react.development.js"></script>
          <script crossorigin src="https://unpkg.com/react-dom@18/umd/react-dom.development.js"></script>
          <script src="https://unpkg.com/@babel/standalone/babel.min.js"></script>
          
          <!-- Chart Library -->
          <script src="https://unpkg.com/recharts@2.8.0/umd/Recharts.js"></script>
          
          <!-- Icons -->
          <script src="https://unpkg.com/lucide-react@latest/dist/umd/lucide-react.js"></script>
          
          <!-- Tailwind CSS -->
          <script src="https://cdn.tailwindcss.com"></script>
          
          <!-- Custom Styles -->
          <style>
            body {
              margin: 0;
              font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', sans-serif;
            }
            
            /* Loading animation */
            .loading {
              display: inline-block;
              width: 20px;
              height: 20px;
              border: 3px solid #f3f3f3;
              border-top: 3px solid #3498db;
              border-radius: 50%;
              animation: spin 2s linear infinite;
            }
            
            @keyframes spin {
              0% { transform: rotate(0deg); }
              100% { transform: rotate(360deg); }
            }
            
            /* Smooth transitions */
            .card-hover {
              transition: all 0.3s ease;
            }
            
            .card-hover:hover {
              transform: translateY(-2px);
              box-shadow: 0 10px 25px rgba(0,0,0,0.1);
            }
          </style>
      </head>
      <body>
          <!-- React Mount Point -->
          <div id="root">
            <div class="min-h-screen bg-gray-100 flex items-center justify-center">
              <div class="text-center">
                <div class="loading"></div>
                <p class="mt-4 text-gray-600">Loading dashboard...</p>
              </div>
            </div>
          </div>
          
          <!-- React Component -->
          <script type="text/babel">
              ${dashboardComponent}
              
              // Render the dashboard
              const root = ReactDOM.createRoot(document.getElementById('root'));
              root.render(<GoogleAdsDashboard />);
          </script>
          
          <!-- Error Handling -->
          <script>
            window.onerror = function(msg, url, line, col, error) {
              console.error('Dashboard Error:', msg, 'at', url, ':', line);
              document.getElementById('root').innerHTML = 
                '<div class="min-h-screen bg-red-50 flex items-center justify-center">' +
                '<div class="text-center p-8">' +
                '<h1 class="text-2xl font-bold text-red-600 mb-4">Dashboard Error</h1>' +
                '<p class="text-red-500 mb-2">' + msg + '</p>' +
                '<p class="text-gray-500 text-sm">Check console for details</p>' +
                '</div></div>';
              return true;
            };
          </script>
      </body>
      </html>
    `;
    
    res.send(html);
    
  } catch (error) {
    console.error('❌ Dashboard server error:', error.message);
    
    // Send error page
    res.status(500).send(`
      <!DOCTYPE html>
      <html>
      <head>
        <title>Dashboard Error</title>
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <style>
          body { font-family: Arial, sans-serif; margin: 40px; background: #f5f5f5; }
          .error-container { background: white; padding: 30px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
          .error-title { color: #e74c3c; margin-bottom: 20px; }
          .error-message { background: #ffeaea; padding: 15px; border-radius: 4px; border-left: 4px solid #e74c3c; }
          .back-link { margin-top: 20px; display: inline-block; color: #3498db; text-decoration: none; }
          .back-link:hover { text-decoration: underline; }
        </style>
      </head>
      <body>
        <div class="error-container">
          <h1 class="error-title">🚨 Dashboard Loading Error</h1>
          <div class="error-message">
            <strong>Error:</strong> ${error.message}
          </div>
          <p><strong>Possible causes:</strong></p>
          <ul>
            <li>Missing dashboard component file</li>
            <li>React component syntax error</li>
            <li>External library loading failure</li>
          </ul>
          <a href="/gads/" class="back-link">← Back to Main Dashboard</a>
        </div>
      </body>
      </html>
    `);
  }
}

/**
 * Health check for dashboard dependencies
 */
function checkDashboardHealth() {
  const checks = {
    dashboard_component: false,
    component_syntax: false
  };
  
  try {
    // Check if dashboard component exists
    const dashboardPath = path.join(__dirname, 'dashboard.js');
    checks.dashboard_component = fs.existsSync(dashboardPath);
    
    if (checks.dashboard_component) {
      // Basic syntax check (just read the file)
      const content = fs.readFileSync(dashboardPath, 'utf8');
      checks.component_syntax = content.includes('GoogleAdsDashboard') && content.includes('React');
    }
    
  } catch (error) {
    console.error('Dashboard health check failed:', error.message);
  }
  
  return {
    healthy: Object.values(checks).every(check => check === true),
    checks: checks,
    timestamp: new Date().toISOString()
  };
}

module.exports = {
  serveDashboard,
  checkDashboardHealth
};