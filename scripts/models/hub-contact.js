/**
 * Contact Model - Data Access Layer
 * Centralizes all contact-related database operations
 * File: /home/hub/public_html/gads/scripts/models/hub-contact.js
 */
class ContactModel {
  constructor(getDbConnection) {
    this.getDbConnection = getDbConnection;
  }

/**
 * Insert or update a contact from HubSpot
 * FIXED: Store country_name instead of country_code
 */
async upsertContact(contactData) {
  const connection = await this.getDbConnection();
  
  try {
    const query = `
      INSERT INTO hub_contacts (
        hubspot_id, name, email, country_name, pipeline_stage, 
        source, hubspot_created_at, hubspot_updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      ON DUPLICATE KEY UPDATE
        name = VALUES(name),
        country_name = VALUES(country_name),
        pipeline_stage = VALUES(pipeline_stage),
        source = VALUES(source),
        hubspot_updated_at = VALUES(hubspot_updated_at),
        updated_at = CURRENT_TIMESTAMP
    `;
    
    const [result] = await connection.execute(query, [
      contactData.hubspotId,
      contactData.name,
      contactData.email,
      contactData.countryName,  // CHANGED: from countryCode to countryName
      contactData.pipelineStage,
      contactData.source,
      contactData.createdAt,
      contactData.updatedAt
    ]);
    
    return result;
  } finally {
    await connection.end();
  }
}

  /**
   * Get contacts by pipeline stage
   */
  async getContactsByPipelineStage() {
    const connection = await this.getDbConnection();
    
    try {
      const [rows] = await connection.execute(`
        SELECT 
          ps.stage_name,
          COUNT(c.contact_id) as count,
          ps.stage_weight,
          ROUND(COUNT(c.contact_id) * 100.0 / (
            SELECT COUNT(*) FROM hub_contacts WHERE contact_id IS NOT NULL
          ), 2) as percentage
        FROM pipeline_stages ps
        LEFT JOIN hub_contacts c ON ps.stage_id = c.pipeline_stage
        GROUP BY ps.stage_id, ps.stage_name, ps.stage_weight
        ORDER BY ps.stage_id
      `);
      
      return rows;
    } finally {
      await connection.end();
    }
  }

  /**
   * Get recent contacts activity
   */
  async getRecentActivity(hours = 24) {
    const connection = await this.getDbConnection();
    
    try {
      const [rows] = await connection.execute(`
        SELECT 
          COUNT(*) as contacts_today,
          COUNT(CASE WHEN pipeline_stage >= 3 THEN 1 END) as responsive_today,
          COUNT(CASE WHEN pipeline_stage = 7 THEN 1 END) as won_today
        FROM hub_contacts 
        WHERE hubspot_created_at >= DATE_SUB(NOW(), INTERVAL ? HOUR)
      `, [hours]);
      
      return rows[0];
    } finally {
      await connection.end();
    }
  }

  /**
   * Get territory burn rate analysis
   */
  async getTerritoryBurnRate(days = 30) {
    const connection = await this.getDbConnection();
    
    try {
      const [rows] = await connection.execute(`
        SELECT 
          COUNT(CASE WHEN cr.status = 'red' THEN 1 END) as red_contacts,
          COUNT(CASE WHEN cr.status = 'yellow' THEN 1 END) as yellow_contacts,
          COUNT(CASE WHEN cr.status = 'green' THEN 1 END) as green_contacts,
          COUNT(*) as total_contacts,
          ROUND(COUNT(CASE WHEN cr.status = 'red' THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 2) as burn_rate
        FROM hub_contacts c
        JOIN country_rules cr ON c.country_code = cr.country_code
        WHERE c.hubspot_created_at >= DATE_SUB(CURDATE(), INTERVAL ? DAY)
      `, [days]);
      
      return rows[0];
    } finally {
      await connection.end();
    }
  }

  /**
   * Get top countries by contact volume
   */
  async getTopCountries(days = 30, limit = 10) {
    const connection = await this.getDbConnection();
    
    try {
      const [rows] = await connection.execute(`
        SELECT 
          cr.country_name,
          cr.status,
          COUNT(c.contact_id) as contact_count,
          COUNT(CASE WHEN c.pipeline_stage >= 3 THEN 1 END) as responsive_count,
          ROUND(COUNT(CASE WHEN c.pipeline_stage >= 3 THEN 1 END) * 100.0 / NULLIF(COUNT(c.contact_id), 0), 2) as responsive_rate
        FROM country_rules cr
        LEFT JOIN hub_contacts c ON cr.country_code = c.country_code 
          AND c.hubspot_created_at >= DATE_SUB(CURDATE(), INTERVAL ? DAY)
        WHERE c.contact_id IS NOT NULL
        GROUP BY cr.country_code, cr.country_name, cr.status
        ORDER BY contact_count DESC
        LIMIT ?
      `, [days, limit]);
      
      return rows;
    } catch (error) {
      console.error('getTopCountries error:', error.message);
      return [];
    } finally {
      await connection.end();
    }
  }

  /**
   * Get territory distribution by status
   */
  async getTerritoryDistribution() {
    const connection = await this.getDbConnection();
    
    try {
      const [rows] = await connection.execute(`
        SELECT 
          cr.status,
          COUNT(c.contact_id) as count,
          ROUND(COUNT(c.contact_id) * 100.0 / (
            SELECT COUNT(*) FROM hub_contacts WHERE country_code IS NOT NULL
          ), 2) as percentage
        FROM country_rules cr
        LEFT JOIN hub_contacts c ON cr.country_code = c.country_code
        GROUP BY cr.status
        ORDER BY 
          CASE cr.status 
            WHEN 'green' THEN 1 
            WHEN 'yellow' THEN 2 
            WHEN 'red' THEN 3 
          END
      `);
      
      return rows;
    } finally {
      await connection.end();
    }
  }

  /**
   * Get data quality metrics
   */
  async getDataQuality() {
    const connection = await this.getDbConnection();
    
    try {
      const [rows] = await connection.execute(`
        SELECT 
          COUNT(*) as total_contacts,
          COUNT(CASE WHEN email IS NULL OR email = '' THEN 1 END) as missing_email,
          COUNT(CASE WHEN country_code IS NULL THEN 1 END) as missing_country,
          COUNT(CASE WHEN pipeline_stage IS NULL THEN 1 END) as missing_stage
        FROM hub_contacts
      `);
      
      const quality = rows[0];
      quality.quality_score = quality.total_contacts > 0 ? Math.round(
        ((quality.total_contacts - quality.missing_email - quality.missing_country - quality.missing_stage) 
         / quality.total_contacts) * 100
      ) : 0;
      
      return quality;
    } finally {
      await connection.end();
    }
  }

  /**
   * Get sync health metrics
   */
  async getSyncHealth() {
    const connection = await this.getDbConnection();
    
    try {
      const [rows] = await connection.execute(`
        SELECT 
          MAX(hubspot_created_at) as last_contact_sync,
          COUNT(*) as contacts_last_hour
        FROM hub_contacts 
        WHERE hubspot_created_at >= DATE_SUB(NOW(), INTERVAL 1 HOUR)
      `);
      
      return rows[0];
    } finally {
      await connection.end();
    }
  }

  /**
   * Get pipeline conversion rates
   */
  async getPipelineConversions(days = 30) {
    const connection = await this.getDbConnection();
    
    try {
      const [rows] = await connection.execute(`
        SELECT 
          ps.stage_name,
          ps.stage_weight,
          COUNT(c.contact_id) as current_count,
          LAG(COUNT(c.contact_id)) OVER (ORDER BY ps.stage_weight) as previous_count,
          ROUND(
            COUNT(c.contact_id) * 100.0 / 
            NULLIF(LAG(COUNT(c.contact_id)) OVER (ORDER BY ps.stage_weight), 0), 2
          ) as conversion_rate
        FROM pipeline_stages ps
        LEFT JOIN hub_contacts c ON ps.stage_id = c.pipeline_stage 
          AND c.hubspot_created_at >= DATE_SUB(CURDATE(), INTERVAL ? DAY)
        WHERE ps.stage_weight > 0
        GROUP BY ps.stage_id, ps.stage_name, ps.stage_weight
        ORDER BY ps.stage_weight
      `, [days]);
      
      return rows;
    } finally {
      await connection.end();
    }
  }

  /**
   * Get overview statistics
   */
  async getOverviewStats() {
    const connection = await this.getDbConnection();
    
    try {
      const [rows] = await connection.execute(`
        SELECT 
          COUNT(*) as total_contacts,
          COUNT(CASE WHEN hubspot_created_at >= DATE_SUB(NOW(), INTERVAL 24 HOUR) THEN 1 END) as contacts_today,
          COUNT(CASE WHEN hubspot_created_at >= DATE_SUB(NOW(), INTERVAL 7 DAY) THEN 1 END) as contacts_week,
          MIN(hubspot_created_at) as oldest_contact,
          MAX(hubspot_created_at) as newest_contact
        FROM hub_contacts
      `);
      
      return rows[0];
    } finally {
      await connection.end();
    }
  }
}

module.exports = ContactModel;