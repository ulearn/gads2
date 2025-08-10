/**
 * Dynamic Field Mapping Module - Production Version
 * Creates every HubSpot field in MySQL with exact field names
 * Minimal logging for production use
 */

//=============================================================================//
//   SIMPLE CONFIGURATION - Just table names and basics
//=============================================================================//

const TABLE_CONFIGS = {
  contacts: {
    tableName: 'hub_contacts',
    primaryKey: 'contact_id',
    hubspotIdField: 'hubspot_id'
  },
  deals: {
    tableName: 'hub_deals',
    primaryKey: 'deal_id',
    hubspotIdField: 'hubspot_deal_id'
  }
};

//=============================================================================//
//   DYNAMIC FIELD HANDLER FUNCTIONS
//=============================================================================//

/**
 * Check if a column exists in the table and add it if missing
 */
async function ensureColumnExists(connection, tableName, hubspotFieldName, fieldValue) {
  try {
    // Check if column exists
    const [columns] = await connection.execute(
      `SELECT COLUMN_NAME 
       FROM INFORMATION_SCHEMA.COLUMNS 
       WHERE TABLE_SCHEMA = ? 
       AND TABLE_NAME = ? 
       AND COLUMN_NAME = ?`,
      [process.env.DB_NAME, tableName, hubspotFieldName]
    );
    
    if (columns.length === 0) {
      // Column doesn't exist, add it
      const dataType = getMySQLDataType(hubspotFieldName, fieldValue);
      
      await connection.execute(
        `ALTER TABLE ${tableName} ADD COLUMN \`${hubspotFieldName}\` ${dataType} DEFAULT NULL`
      );
      
      console.log(`   ✅ Added column: ${hubspotFieldName} (${dataType})`);
    }
    
    return hubspotFieldName;
  } catch (error) {
    console.error(`   ❌ Could not add column ${hubspotFieldName}: ${error.message}`);
    return null;
  }
}

/**
 * Determine MySQL data type based on value
 */
/**
 * Determine MySQL data type based on value - DEFAULT TO TEXT
 */
function getMySQLDataType(fieldName, value) {
  // Analyze the actual value to determine type
  if (value !== null && value !== undefined && value !== '') {
    
    // Check if it's a timestamp (HubSpot often uses milliseconds)
    if (typeof value === 'string' && /^\d{13}$/.test(value)) {
      return 'BIGINT';
    }
    
    // Check if it's a date string
    if (typeof value === 'string' && Date.parse(value) && (value.includes('T') || value.includes('-'))) {
      return 'DATETIME';
    }
    
    // Check if it's a boolean
    if (value === 'true' || value === 'false' || typeof value === 'boolean') {
      return 'BOOLEAN';
    }
    
    // Check if it's a number
    if (!isNaN(value) && value !== '') {
      const numValue = Number(value);
      
      if (numValue % 1 !== 0) {
        return 'DECIMAL(15,6)';
      }
      
      if (numValue > 2147483647 || numValue < -2147483648) {
        return 'BIGINT';
      } else {
        return 'INT';
      }
    }
  }
  
  // DEFAULT TO TEXT FOR ALL STRING FIELDS (avoids row size limits)
  return 'TEXT';
}

/**
 * Transform field values for MySQL storage
 */
function transformValue(fieldName, value) {
  if (value === null || value === undefined || value === '') {
    return null;
  }
  
  // Handle HubSpot timestamps
  if (typeof value === 'string' && /^\d{13}$/.test(value)) {
    return parseInt(value);
  }
  
  // Handle date strings
  if (typeof value === 'string' && Date.parse(value) && (value.includes('T') || value.includes('-'))) {
    try {
      return new Date(value);
    } catch (error) {
      return value;
    }
  }
  
  // Handle booleans
  if (value === 'true' || value === 'false') {
    return value === 'true';
  }
  
  // Handle numbers
  if (!isNaN(value) && value !== '') {
    return Number(value);
  }
  
  return value;
}

//=============================================================================//
//   TABLE INITIALIZATION FUNCTIONS
//=============================================================================//

async function ensureTableExists(connection, objectType) {
  try {
    const config = TABLE_CONFIGS[objectType];
    if (!config) {
      throw new Error(`Unknown object type: ${objectType}`);
    }
    
    const { tableName, primaryKey, hubspotIdField } = config;
    
    const createSQL = `
      CREATE TABLE IF NOT EXISTS ${tableName} (
        ${primaryKey} INT AUTO_INCREMENT PRIMARY KEY,
        ${hubspotIdField} VARCHAR(50) UNIQUE NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        KEY idx_hubspot_id (${hubspotIdField})
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    `;
    
    await connection.execute(createSQL);
    console.log(`✅ ${tableName} table ready`);
    
    return config;
  } catch (error) {
    console.error(`❌ Failed to initialize table for ${objectType}:`, error.message);
    throw error;
  }
}

//=============================================================================//
//   DYNAMIC PROCESSING WITH MINIMAL LOGGING
//=============================================================================//

async function processHubSpotObject(hubspotObject, connection, objectType) {
  try {
    const config = TABLE_CONFIGS[objectType];
    if (!config) {
      throw new Error(`Unknown object type: ${objectType}`);
    }
    
    const { tableName, hubspotIdField } = config;
    
    // Start with HubSpot ID
    const data = {};
    data[hubspotIdField] = hubspotObject.id;
    
    let processedFields = 0;
    
    // Process ALL properties that have values
    if (hubspotObject.properties) {
      for (const [hubspotFieldName, fieldValue] of Object.entries(hubspotObject.properties)) {
        // Skip empty values
        if (fieldValue === null || fieldValue === undefined || fieldValue === '') {
          continue;
        }
        
        try {
          // Ensure column exists
          const columnName = await ensureColumnExists(connection, tableName, hubspotFieldName, fieldValue);
          
          if (columnName) {
            // Transform and store value
            const transformedValue = transformValue(hubspotFieldName, fieldValue);
            data[columnName] = transformedValue;
            processedFields++;
          }
        } catch (error) {
          console.error(`❌ Error processing field ${hubspotFieldName}:`, error.message);
        }
      }
    }
    
    // Build and execute query
    const columns = Object.keys(data);
    const values = Object.values(data);
    const placeholders = columns.map(() => '?').join(', ');
    const updateClauses = columns
      .filter(col => col !== hubspotIdField)
      .map(col => `\`${col}\` = VALUES(\`${col}\`)`)
      .join(', ');
    
    const query = `
      INSERT INTO ${tableName} (${columns.map(c => `\`${c}\``).join(', ')})
      VALUES (${placeholders})
      ON DUPLICATE KEY UPDATE ${updateClauses}
    `;
    
    await connection.execute(query, values);
    
    if (processedFields > 0) {
      console.log(`✅ Saved ${objectType} ${hubspotObject.id} (${processedFields} fields)`);
    }
    
    return true;
  } catch (error) {
    console.error(`❌ Failed to process ${objectType} ${hubspotObject.id}:`, error.message);
    return false;
  }
}

//=============================================================================//
//   EXPORTED FUNCTIONS
//=============================================================================//

module.exports = {
  ensureTableExists,
  ensureColumnExists,
  processHubSpotObject,
  getMySQLDataType,
  transformValue,
  TABLE_CONFIGS
};