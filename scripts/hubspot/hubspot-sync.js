/**
 * Enhanced HubSpot Sync with Contact-Deal Associations using API v4
 * /scripts/hubspot/hubspot-sync.js
 * 
 * Now captures the association data between contacts and deals using Associations API v4
 */

const fieldMap = require('./fieldmap');

/**
 * Create association table if it doesn't exist
 */
async function ensureAssociationTableExists(connection) {
  try {
    const createAssociationTable = `
      CREATE TABLE IF NOT EXISTS hub_contact_deal_associations (
        association_id INT AUTO_INCREMENT PRIMARY KEY,
        contact_hubspot_id VARCHAR(50) NOT NULL,
        deal_hubspot_id VARCHAR(50) NOT NULL,
        association_type VARCHAR(50) DEFAULT 'contact_to_deal',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        
        INDEX idx_contact_id (contact_hubspot_id),
        INDEX idx_deal_id (deal_hubspot_id),
        UNIQUE KEY unique_contact_deal (contact_hubspot_id, deal_hubspot_id)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    `;
    
    await connection.execute(createAssociationTable);
    console.log('✅ Association table ready');
    
  } catch (error) {
    console.error('❌ Failed to create association table:', error.message);
    throw error;
  }
}

/**
 * Save contact-deal associations
 */
async function saveContactAssociations(connection, contactId, associations) {
  if (!associations || associations.length === 0) {
    return 0;
  }
  
  try {
    let savedCount = 0;
    
    for (const dealId of associations) {
      try {
        await connection.execute(`
          INSERT INTO hub_contact_deal_associations 
          (contact_hubspot_id, deal_hubspot_id) 
          VALUES (?, ?)
          ON DUPLICATE KEY UPDATE updated_at = CURRENT_TIMESTAMP
        `, [contactId, dealId]);
        
        savedCount++;
        
      } catch (error) {
        console.error(`   ⚠️ Failed to save association ${contactId} → ${dealId}:`, error.message);
      }
    }
    
    return savedCount;
    
  } catch (error) {
    console.error(`❌ Error saving associations for contact ${contactId}:`, error.message);
    return 0;
  }
}

/**
 * FIXED: Enhanced sync function that captures associations
 * The issue was missing 'associations' parameter in the API calls
 */
async function syncObjectsWithAllPropertiesAndAssociations(hubspotClient, connection, objectType, startDate, endDate, allPropertyNames) {
  try {
    console.log(`🔄 Syncing ${objectType} with associations (${allPropertyNames.length} properties)...`);
    
    let after = undefined;
    let totalSynced = 0;
    let totalAssociations = 0;
    let page = 1;
    
    while (true) {
      let response;
      
      if (objectType === 'contacts') {
        response = await hubspotClient.crm.contacts.searchApi.doSearch({
          filterGroups: [{
            filters: [
              {
                propertyName: 'createdate',
                operator: 'BETWEEN',
                value: startDate.getTime().toString(),
                highValue: endDate.getTime().toString()
              },
              {
                propertyName: 'hs_object_source',
                operator: 'NEQ',
                value: 'IMPORT'
              }
            ]
          }],
          properties: allPropertyNames,
          associations: ['deals'], // 🔥 CRITICAL FIX: This was missing!
          limit: 100,
          after: after
        });
      } else if (objectType === 'deals') {
        response = await hubspotClient.crm.deals.searchApi.doSearch({
          filterGroups: [{
            filters: [{
              propertyName: 'createdate',
              operator: 'BETWEEN', 
              value: startDate.getTime().toString(),
              highValue: endDate.getTime().toString()
            }]
          }],
          properties: allPropertyNames,
          associations: ['contacts'], // 🔥 ALSO ADD for deals
          limit: 100,
          after: after
        });
      }
      
      const objects = response.results || [];
      
      if (objects.length === 0) {
        break;
      }
      
      console.log(`   📄 Page ${page}: Processing ${objects.length} ${objectType}...`);
      
      // Process objects
      for (const obj of objects) {
        // Save the main object (contact or deal)
        const success = await fieldMap.processHubSpotObject(obj, connection, objectType);
        
        if (success) {
          totalSynced++;
          
          // For contacts: save associations to deals
          // CORRECT (fixed code):
          if (objectType === 'contacts') {
            const dealAssociations = obj.associations || [];
            console.log(`   🔗 Contact ${obj.id} has ${dealAssociations.length} deal associations:`, dealAssociations);
            
            if (dealAssociations.length > 0) {
              const associationCount = await saveContactAssociations(
                connection, 
                obj.id, 
                dealAssociations
              );
              totalAssociations += associationCount;
            }
          }
        }
      }
      
      
      after = response.paging?.next?.after;
      if (!after) {
        break;
      }
      
      page++;
      await delay(100); // Rate limiting
    }
    
    if (objectType === 'contacts') {
      console.log(`✅ ${objectType} sync complete: ${totalSynced} records, ${totalAssociations} associations`);
    } else {
      console.log(`✅ ${objectType} sync complete: ${totalSynced} records`);
    }
    
    return { synced: totalSynced, associations: totalAssociations };
    
  } catch (error) {
    console.error(`❌ ${objectType} sync failed:`, error.message);
    throw error;
  }
}

// Properties to SKIP - these cause row size issues
const SKIP_PROPERTIES = [
  'hs_user_ids_of_all_notification_followers',
  'hs_user_ids_of_all_notification_unfollowers', 
  'industry',
  'instagram',
  'ip_latlon',
  'ip_zipcode',
  'job_function',
  'linkedinbio',
  'marital_status',
  'markets',
  'military_status',
  'nick',
  'numemployees',
  'owneremail',
  'ownername',
  'partner_tags',
  'phone_2',
  'photo',
  'preferred_period_to_study',
  'relationship_status',
  'salutation',
  'school',
  'seniority',
  'student_id',
  'tiktok',
  'twitterbio',
  'twitterhandle',
  'twitterprofilephoto',
  'website',
  'work_email',
  
  'hs_gps_error',
  'hs_gps_latitude',
  'hs_gps_longitude',
  'hs_inferred_language_codes',
  'hs_journey_stage',
  'hs_language',
  'hs_linkedin_ad_clicked',
  'hs_linkedin_url',
  'hs_mobile_sdk_push_tokens',
  'hs_persona',
  'hs_predictivecontactscorebucket',
  'hs_predictivescoringtier',
  'hs_registration_method',
  'hs_shared_team_ids',
  'hs_shared_user_ids',
  'hs_state_code',
  'hs_sub_role',
  'hs_testpurge',
  'hs_testrollback',
  'hs_unique_creation_key'
];

/**
 * Get ALL available properties but filter out problematic ones
 */
async function getAllAvailableProperties(hubspotClient, objectType) {
  try {
    console.log(`🔍 Getting ${objectType} properties from HubSpot...`);
    
    const response = await hubspotClient.crm.properties.coreApi.getAll(objectType);
    const allProperties = response.results.map(prop => ({
      name: prop.name,
      type: prop.type,
      fieldType: prop.fieldType
    }));
    
    // Filter out problematic properties for contacts
    let filteredProperties = allProperties;
    if (objectType === 'contacts') {
      filteredProperties = allProperties.filter(prop => 
        !SKIP_PROPERTIES.includes(prop.name)
      );
      
      const skippedCount = allProperties.length - filteredProperties.length;
      console.log(`⚠️ Skipped ${skippedCount} problematic properties to avoid row size limits`);
    }
    
    console.log(`✅ Found ${allProperties.length} ${objectType} properties, using ${filteredProperties.length} safe ones`);
    
    return filteredProperties;
    
  } catch (error) {
    console.error(`❌ Failed to get ${objectType} properties:`, error.message);
    throw error;
  }
}

/**
 * Get existing MySQL table columns
 */
async function getExistingColumns(connection, tableName) {
  try {
    const [columns] = await connection.execute(
      `SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS 
       WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?`,
      [tableName]
    );
    
    return columns.map(col => col.COLUMN_NAME);
    
  } catch (error) {
    console.error(`❌ Failed to get existing columns for ${tableName}:`, error.message);
    throw error;
  }
}

/**
 * Map HubSpot property type to MySQL column type
 * Fixed: Use TEXT instead of VARCHAR to avoid row size limits
 */
function getColumnType(hubspotPropertyType, fieldType) {
  switch (hubspotPropertyType) {
    case 'string':
      return 'TEXT';  // Changed: Always use TEXT instead of VARCHAR(255)
    case 'number':
      return 'DECIMAL(15,2)';
    case 'bool':
    case 'boolean':
      return 'BOOLEAN';
    case 'datetime':
    case 'date':
      return 'DATETIME';
    case 'enumeration':
      return 'TEXT';  // Changed: Use TEXT instead of VARCHAR(255)
    default:
      return 'TEXT';
  }
}

/**
 * Add missing columns to MySQL table
 */
async function addMissingColumns(connection, tableName, missingColumns) {
  if (missingColumns.length === 0) {
    console.log(`✅ ${tableName}: Schema up to date`);
    return;
  }

  console.log(`🔧 ${tableName}: Adding ${missingColumns.length} missing columns...`);
  
  for (const column of missingColumns) {
    try {
      const columnType = getColumnType(column.type, column.fieldType);
      const alterQuery = `ALTER TABLE ${tableName} ADD COLUMN \`${column.name}\` ${columnType}`;
      
      await connection.execute(alterQuery);
      console.log(`   ✅ Added column: ${column.name}`);
      
    } catch (error) {
      console.error(`   ❌ Failed to add column ${column.name}:`, error.message);
    }
  }
}

/**
 * Sync HubSpot schema with MySQL tables
 */
async function syncSchema(hubspotClient, getDbConnection) {
  try {
    console.log('🚀 Starting schema sync...');
    
    const connection = await getDbConnection();
    
    try {
      // Initialize tables
      await fieldMap.ensureTableExists(connection, 'contacts');
      await fieldMap.ensureTableExists(connection, 'deals');
      
      // NEW: Initialize association table
      await ensureAssociationTableExists(connection);
      
      // Process contacts schema
      const contactProperties = await getAllAvailableProperties(hubspotClient, 'contacts');
      const existingContactColumns = await getExistingColumns(connection, 'hub_contacts');
      
      const missingContactColumns = contactProperties.filter(
        prop => !existingContactColumns.includes(prop.name)
      );
      
      console.log(`📊 Contacts: ${contactProperties.length} properties, ${missingContactColumns.length} missing`);
      await addMissingColumns(connection, 'hub_contacts', missingContactColumns);
      
      // Process deals schema
      const dealProperties = await getAllAvailableProperties(hubspotClient, 'deals');
      const existingDealColumns = await getExistingColumns(connection, 'hub_deals');
      
      const missingDealColumns = dealProperties.filter(
        prop => !existingDealColumns.includes(prop.name)
      );
      
      console.log(`📊 Deals: ${dealProperties.length} properties, ${missingDealColumns.length} missing`);
      await addMissingColumns(connection, 'hub_deals', missingDealColumns);
      
      console.log('✅ Schema sync completed');
      
      return {
        success: true,
        contacts: {
          hubspot_properties: contactProperties.length,
          columns_added: missingContactColumns.length
        },
        deals: {
          hubspot_properties: dealProperties.length,
          columns_added: missingDealColumns.length
        },
        associations_table: 'ready',
        timestamp: new Date().toISOString()
      };
      
    } finally {
      await connection.end();
    }
    
  } catch (error) {
    console.error('💥 Schema sync failed:', error.message);
    throw error;
  }
}

/**
 * NEW: Sync contact-deal associations using the Associations API v4
 * This runs AFTER contacts and deals are synced
 */
async function syncContactDealAssociations(hubspotClient, connection) {
  try {
    console.log('🔗 Starting contact-deal associations sync using Associations API v4...');
    
    // Step 1: Get all contact IDs from our database that have deals
    const [contacts] = await connection.execute(`
      SELECT hubspot_id 
      FROM hub_contacts 
      WHERE num_associated_deals > 0
      LIMIT 1000
    `);
    
    if (contacts.length === 0) {
      console.log('   ⚠️ No contacts with associated deals found');
      return { success: true, associations: 0 };
    }
    
    console.log(`   📊 Found ${contacts.length} contacts with associated deals`);
    
    // Step 2: Batch query associations using HubSpot Associations API v4
    const contactIds = contacts.map(row => ({ id: row.hubspot_id }));
    let totalAssociations = 0;
    
    // Process in batches of 100 (API limit)
    for (let i = 0; i < contactIds.length; i += 100) {
      const batch = contactIds.slice(i, i + 100);
      
      console.log(`   📦 Processing batch ${Math.floor(i/100) + 1}/${Math.ceil(contactIds.length/100)}`);
      
      try {
        // Use the V3 batch API (V4 doesn't have batch endpoints)
        const response = await hubspotClient.crm.associations.batchApi.read(
          'contacts', // fromObjectType
          'deals',    // toObjectType
          {
            inputs: batch
          }
        );
        
        console.log(`   🔍 API Response for batch:`, {
          status: response.status,
          resultsCount: response.results?.length || 0,
          sampleResult: response.results?.[0] ? JSON.stringify(response.results[0], null, 2) : 'No results'
        });
        
        // Step 3: Process the associations
        if (response.results && response.results.length > 0) {
          for (const result of response.results) {
            console.log(`   🔍 DEBUG: Processing result structure:`, JSON.stringify(result, null, 2));
            
            // FIXED: Parse the correct structure from V3 API
            const contactId = result._from.id;
            const dealAssociations = result.to || [];
            
            console.log(`   🔗 Contact ${contactId} has ${dealAssociations.length} deal associations`);
            
            // Save each association
            for (const dealAssoc of dealAssociations) {
              const dealId = dealAssoc.id;
              
              try {
                await connection.execute(`
                  INSERT IGNORE INTO hub_contact_deal_associations 
                  (contact_hubspot_id, deal_hubspot_id, association_type) 
                  VALUES (?, ?, ?)
                `, [contactId, dealId, 'primary']);
                
                totalAssociations++;
                
              } catch (error) {
                console.error(`     ❌ Failed to save association ${contactId} → ${dealId}:`, error.message);
              }
            }
          }
        }
        
        // Rate limiting
        await delay(200);
        
      } catch (error) {
        console.error(`   ❌ Failed to fetch associations for batch:`, error.message);
      }
    }
    
    console.log(`✅ Associations sync complete: ${totalAssociations} associations saved`);
    
    return {
      success: true,
      associations: totalAssociations,
      contacts_processed: contacts.length
    };
    
  } catch (error) {
    console.error('❌ Associations sync failed:', error.message);
    throw error;
  }
}

/**
 * Enhanced sync function that ensures schema is up-to-date before syncing data WITH associations
 */
async function runSyncWithSchemaCheck(hubspotClient, getDbConnection, options = {}) {
  try {
    // Step 1: Sync schema first
    console.log('🔧 STEP 1: Syncing schema...');
    await syncSchema(hubspotClient, getDbConnection);
    
    // Step 2: Get current complete property lists
    console.log('📋 STEP 2: Getting property lists for data sync...');
    const contactProperties = await getAllAvailableProperties(hubspotClient, 'contacts');
    const dealProperties = await getAllAvailableProperties(hubspotClient, 'deals');
    
    const contactPropertyNames = contactProperties.map(p => p.name);
    const dealPropertyNames = dealProperties.map(p => p.name);
    
    // Step 3: Run data sync
    console.log('🚀 STEP 3: Syncing data...');
    
    // Handle date range
    let startDate, endDate;
    if (options.startDate && options.endDate) {
      startDate = new Date(options.startDate);
      endDate = new Date(options.endDate);
      endDate.setHours(23, 59, 59, 999);
    } else if (options.daysBack) {
      endDate = new Date();
      startDate = new Date();
      startDate.setDate(startDate.getDate() - options.daysBack);
    } else {
      endDate = new Date();
      startDate = new Date();
      startDate.setDate(startDate.getDate() - 365);
    }
    
    console.log(`📅 Date range: ${startDate.toLocaleDateString()} to ${endDate.toLocaleDateString()}`);
    
    const connection = await getDbConnection();
    
    try {
      // Sync contacts and deals
      const contactResult = await syncObjectsWithAllPropertiesAndAssociations(
        hubspotClient, connection, 'contacts', startDate, endDate, contactPropertyNames
      );
      
      const dealResult = await syncObjectsWithAllPropertiesAndAssociations(
        hubspotClient, connection, 'deals', startDate, endDate, dealPropertyNames
      );
      
      // Step 4: NEW - Sync associations using Associations API v4
      console.log('🔗 STEP 4: Syncing contact-deal associations...');
      const associationsResult = await syncContactDealAssociations(hubspotClient, connection);
      
      console.log('🎉 Enhanced sync completed successfully!');
      console.log(`📊 Synced: ${contactResult.synced} contacts, ${dealResult.synced} deals`);
      console.log(`🔗 Contact-Deal Associations: ${associationsResult.associations} via API v4`);
      
      return {
        success: true,
        contacts_synced: contactResult.synced,
        deals_synced: dealResult.synced,
        associations_synced: associationsResult.associations,
        contact_properties_used: contactPropertyNames.length,
        deal_properties_used: dealPropertyNames.length,
        date_range: {
          start: startDate.toISOString(),
          end: endDate.toISOString()
        },
        timestamp: new Date().toISOString()
      };
      
    } finally {
      await connection.end();
    }
    
  } catch (error) {
    console.error('💥 Enhanced sync failed:', error.message);
    throw error;
  }
}

function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

module.exports = { 
  syncSchema,
  runSyncWithSchemaCheck,
  getAllAvailableProperties,
  saveContactAssociations,
  ensureAssociationTableExists,
  syncContactDealAssociations
};