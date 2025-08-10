const mysql = require('mysql2/promise');
const fs = require('fs').promises;
require('dotenv').config({ path: '../../.env' });

/**
 * Load country data from the merged country-codes.json file into the database
 */
async function loadCountryData() {
  let connection;
  
  try {
    console.log('🔄 Loading country data from merged JSON file...');
    
    // Read the merged country codes JSON file (with both code and territory)
    const countryData = await fs.readFile('./country-codes.json', 'utf8');
    const countriesMap = JSON.parse(countryData);
    
    console.log(`📊 Found ${Object.keys(countriesMap).length} countries in merged file`);
    
    // Create database connection
    connection = await mysql.createConnection({
      host: process.env.DB_HOST || 'localhost',
      port: process.env.DB_PORT || 3306,
      user: process.env.DB_USER,
      password: process.env.DB_PASSWORD,
      database: process.env.DB_NAME
    });
    
    console.log('✅ Database connection established');
    
    // Clear existing country data
    await connection.execute('DELETE FROM country_rules');
    console.log('🗑️  Cleared existing country data');
    
    // Prepare country data for insertion
    const insertData = [];
    let skippedCount = 0;
    
    for (const [countryName, countryInfo] of Object.entries(countriesMap)) {
      const { code: countryCode, territory: classification } = countryInfo;
      
      // Skip countries without country codes
      if (!countryCode || countryCode.trim() === '') {
        console.warn(`⚠️  No country code for: ${countryName} - skipping`);
        skippedCount++;
        continue;
      }
      
      // Map your classifications to our green/yellow/red system
      let status, visaRequired;
      
      switch (classification) {
        case 'EU':
          status = 'green';
          visaRequired = false;
          break;
        case 'Non-EU (No Visa)':
          status = 'green';
          visaRequired = false;
          break;
        case 'Non-EU (VBD)': // Visa Before Departure
          status = 'yellow';
          visaRequired = true;
          break;
        case 'Unsupported Territory':
          status = 'red';
          visaRequired = true;
          break;
        default:
          console.warn(`⚠️  Unknown classification for ${countryName}: ${classification}`);
          status = 'red'; // Default to red for unknown
          visaRequired = true;
      }
      
      insertData.push([
        countryCode,
        countryName,
        status,
        visaRequired,
        classification // Store original classification in notes
      ]);
    }
    
    console.log(`📋 Prepared ${insertData.length} countries for insertion (skipped ${skippedCount} without codes)`);
    
    // Insert country data
    const insertQuery = `
      INSERT INTO country_rules (country_code, country_name, status, visa_required, notes)
      VALUES (?, ?, ?, ?, ?)
    `;
    
    let insertedCount = 0;
    for (const country of insertData) {
      try {
        await connection.execute(insertQuery, country);
        insertedCount++;
      } catch (error) {
        if (error.code === 'ER_DUP_ENTRY') {
          console.warn(`⚠️  Duplicate country code: ${country[1]} (${country[0]})`);
        } else {
          console.error(`❌ Error inserting ${country[1]}:`, error.message);
        }
      }
    }
    
    console.log(`✅ Inserted ${insertedCount} countries into database`);
    
    // Show summary by status
    const [summary] = await connection.execute(`
      SELECT status, COUNT(*) as count 
      FROM country_rules 
      GROUP BY status 
      ORDER BY status
    `);
    
    console.log('\n📊 Country Classification Summary:');
    summary.forEach(row => {
      const emoji = row.status === 'green' ? '🟢' : row.status === 'yellow' ? '🟡' : '🔴';
      console.log(`   ${emoji} ${row.status.toUpperCase()}: ${row.count} countries`);
    });
    
    // Show some examples
    console.log('\n🔍 Examples by classification:');
    const [examples] = await connection.execute(`
      SELECT status, GROUP_CONCAT(country_name SEPARATOR ', ') as examples
      FROM (
        SELECT status, country_name,
               ROW_NUMBER() OVER (PARTITION BY status ORDER BY country_name) as rn
        FROM country_rules
      ) ranked
      WHERE rn <= 5
      GROUP BY status
      ORDER BY status
    `);
    
    examples.forEach(row => {
      const emoji = row.status === 'green' ? '🟢' : row.status === 'yellow' ? '🟡' : '🔴';
      console.log(`   ${emoji} ${row.status.toUpperCase()}: ${row.examples}`);
    });
    
  } catch (error) {
    console.error('❌ Error loading country data:', error.message);
    throw error;
  } finally {
    if (connection) {
      await connection.end();
      console.log('🔌 Database connection closed');
    }
  }
}

/**
 * Verify the loaded data makes sense
 */
async function verifyCountryData() {
  let connection;
  
  try {
    connection = await mysql.createConnection({
      host: process.env.DB_HOST || 'localhost',
      port: process.env.DB_PORT || 3306,
      user: process.env.DB_USER,
      password: process.env.DB_PASSWORD,
      database: process.env.DB_NAME
    });
    
    console.log('\n🔍 Verifying Phase 1 target countries:');
    
    const phase1Countries = ['Brazil', 'Argentina', 'Chile', 'Uruguay'];
    
    for (const country of phase1Countries) {
      const [rows] = await connection.execute(
        'SELECT country_name, status, visa_required, country_code FROM country_rules WHERE country_name = ?',
        [country]
      );
      
      if (rows.length > 0) {
        const { status, visa_required, country_code } = rows[0];
        const emoji = status === 'green' ? '🟢' : status === 'yellow' ? '🟡' : '🔴';
        const visaText = visa_required ? 'Visa Required' : 'No Visa';
        console.log(`   ${emoji} ${country} (${country_code}): ${status.toUpperCase()} (${visaText})`);
      } else {
        console.log(`   ❌ ${country}: NOT FOUND`);
      }
    }
    
  } catch (error) {
    console.error('❌ Error verifying data:', error.message);
  } finally {
    if (connection) {
      await connection.end();
    }
  }
}

// Run the script
if (require.main === module) {
  loadCountryData()
    .then(() => verifyCountryData())
    .then(() => {
      console.log('\n🎉 Country data loading completed successfully!');
      console.log('\nNext steps:');
      console.log('1. Run: node ../../db-test.js (to verify database connection)');
      console.log('2. Check the country_rules table in phpMyAdmin');
      console.log('3. Start building your campaign targeting logic');
    })
    .catch(error => {
      console.error('💥 Script failed:', error.message);
      process.exit(1);
    });
}

module.exports = { loadCountryData, verifyCountryData };