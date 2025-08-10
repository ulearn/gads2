/**
 * Fetch specific HubSpot contact with ALL fields
 * Add this function to your hubspot-test.js or create a new file
 */

/**
 * Fetch a specific contact by ID with all available properties
 * @param {Object} hubspotClient - Authenticated HubSpot client
 * @param {string} contactId - HubSpot contact ID
 * @returns {Object} Complete contact data
 */
async function fetchSpecificContact(hubspotClient, contactId = '347587358941') {
  try {
    console.log(`\n🔍 Fetching Contact ID: ${contactId} with ALL properties...\n`);
    
    const results = {
      contactId: contactId,
      basicData: null,
      allProperties: null,
      associations: null,
      sqlSchema: null
    };

    // Step 1: Get all available contact properties first
    console.log('📋 Step 1: Fetching all available contact properties...');
    const propertiesResponse = await hubspotClient.crm.properties.coreApi.getAll('contacts');
    const propertyNames = propertiesResponse.results.map(p => p.name);
    console.log(`   Found ${propertyNames.length} total properties available`);

    // Step 2: Fetch the contact with ALL properties
    console.log(`\n📧 Step 2: Fetching contact ${contactId} with all properties...`);
    const contact = await hubspotClient.crm.contacts.basicApi.getById(
      contactId,
      propertyNames, // Request ALL properties
      undefined,
      undefined,
      false
    );

    // Store basic data
    results.basicData = {
      id: contact.id,
      createdAt: contact.createdAt,
      updatedAt: contact.updatedAt,
      archived: contact.archived
    };

    // Store all properties (including null ones)
    results.allProperties = contact.properties;

    // Analyze the data
    console.log('\n📊 Contact Data Analysis:');
    console.log('========================');
    
    // Count non-null properties
    const nonNullProps = Object.entries(contact.properties)
      .filter(([key, value]) => value !== null && value !== '' && value !== undefined);
    
    console.log(`✅ Total properties: ${Object.keys(contact.properties).length}`);
    console.log(`✅ Non-null properties: ${nonNullProps.length}`);
    
    // Display key information
    console.log('\n🔑 Key Contact Information:');
    console.log('---------------------------');
    const keyFields = [
      'email', 'firstname', 'lastname', 'phone', 'company',
      'country', 'city', 'state', 'zip',
      'lifecyclestage', 'hs_lead_status', 
      'hs_analytics_source', 'hs_analytics_source_data_1', 'hs_analytics_source_data_2',
      'createdate', 'lastmodifieddate', 'website', 'jobtitle'
    ];
    
    keyFields.forEach(field => {
      if (contact.properties[field]) {
        console.log(`   ${field}: ${contact.properties[field]}`);
      }
    });

    // Display all non-null properties
    console.log('\n📝 ALL Non-Null Properties:');
    console.log('---------------------------');
    nonNullProps.forEach(([key, value]) => {
      // Truncate long values for display
      const displayValue = value && value.length > 100 
        ? value.substring(0, 100) + '...' 
        : value;
      console.log(`   ${key}: ${displayValue}`);
    });

    // Step 3: Get associated deals if any
    console.log('\n🔗 Step 3: Checking for associated deals...');
    try {
      const associations = await hubspotClient.crm.associations.batchApi.read(
        'contacts',
        'deals',
        {
          inputs: [{ id: contactId }]
        }
      );
      
      if (associations.results && associations.results[0] && associations.results[0].to) {
        const dealIds = associations.results[0].to.map(deal => deal.id);
        console.log(`   Found ${dealIds.length} associated deal(s): ${dealIds.join(', ')}`);
        
        // Fetch deal details
        if (dealIds.length > 0) {
          console.log('\n💰 Fetching associated deal details...');
          for (const dealId of dealIds) {
            try {
              const deal = await hubspotClient.crm.deals.basicApi.getById(
                dealId,
                ['dealname', 'amount', 'dealstage', 'closedate', 'pipeline']
              );
              console.log(`   Deal ${dealId}:`);
              console.log(`     Name: ${deal.properties.dealname}`);
              console.log(`     Amount: ${deal.properties.amount}`);
              console.log(`     Stage: ${deal.properties.dealstage}`);
            } catch (error) {
              console.log(`   Could not fetch deal ${dealId}: ${error.message}`);
            }
          }
        }
        
        results.associations = dealIds;
      } else {
        console.log('   No associated deals found');
      }
    } catch (error) {
      console.log('   Could not fetch associations:', error.message);
    }

    // Step 4: Generate SQL schema recommendation
    console.log('\n🗄️  Step 4: Generating SQL schema recommendation...');
    results.sqlSchema = generateContactSQLSchema(nonNullProps);
    console.log(results.sqlSchema);

    // Save to file for reference
    const fs = require('fs').promises;
    const filename = `contact-${contactId}-export.json`;
    await fs.writeFile(filename, JSON.stringify(results, null, 2));
    console.log(`\n💾 Complete data saved to: ${filename}`);

    return results;

  } catch (error) {
    console.error(`\n❌ Failed to fetch contact ${contactId}:`, error.message);
    throw error;
  }
}

/**
 * Generate SQL CREATE TABLE statement based on actual contact data
 */
function generateContactSQLSchema(nonNullProperties) {
  let sql = '-- SQL Schema based on actual HubSpot contact data\n';
  sql += 'CREATE TABLE IF NOT EXISTS hub_leads (\n';
  sql += '  lead_id INT AUTO_INCREMENT PRIMARY KEY,\n';
  sql += '  hubspot_id VARCHAR(50) UNIQUE NOT NULL,\n';
  
  // Map of HubSpot fields to SQL columns
  const fieldMapping = {
    'email': 'VARCHAR(255)',
    'firstname': 'VARCHAR(100)',
    'lastname': 'VARCHAR(100)',
    'phone': 'VARCHAR(50)',
    'company': 'VARCHAR(255)',
    'country': 'VARCHAR(100)',
    'city': 'VARCHAR(100)',
    'state': 'VARCHAR(100)',
    'zip': 'VARCHAR(20)',
    'lifecyclestage': 'VARCHAR(50)',
    'hs_lead_status': 'VARCHAR(50)',
    'hs_analytics_source': 'VARCHAR(100)',
    'hs_analytics_source_data_1': 'VARCHAR(255)',
    'hs_analytics_source_data_2': 'VARCHAR(255)',
    'website': 'VARCHAR(255)',
    'jobtitle': 'VARCHAR(255)',
    'createdate': 'DATETIME',
    'lastmodifieddate': 'DATETIME',
    'hs_object_id': 'BIGINT',
    'notes_last_updated': 'DATETIME',
    'notes_last_contacted': 'DATETIME',
    'num_contacted_notes': 'INT',
    'num_notes': 'INT',
    'hs_email_bounce': 'INT'
  };

  // Add fields that exist in the actual contact
  nonNullProperties.forEach(([field, value]) => {
    if (fieldMapping[field]) {
      sql += `  ${field} ${fieldMapping[field]},\n`;
    }
  });

  sql += '  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,\n';
  sql += '  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n';
  sql += '  KEY idx_email (email),\n';
  sql += '  KEY idx_country (country),\n';
  sql += '  KEY idx_lifecycle (lifecyclestage),\n';
  sql += '  KEY idx_created (createdate)\n';
  sql += ') ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;\n';

  return sql;
}

// Export the function
module.exports = { 
  fetchSpecificContact  // Add new function
};