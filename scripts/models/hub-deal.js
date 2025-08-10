/**
 * Deal Model - Data Access Layer
 * Centralizes all deal-related database operations
 * File: /home/hub/public_html/gads/scripts/models/hub-deal.js
 */
class DealModel {
  constructor(getDbConnection) {
    this.getDbConnection = getDbConnection;
  }

  /**
   * Ensure deals table exists
   */
  async ensureDealsTable() {
    const connection = await this.getDbConnection();
    
    try {
      const createTableQuery = `
        CREATE TABLE IF NOT EXISTS hub_deals (
          deal_id INT AUTO_INCREMENT PRIMARY KEY,
          hubspot_deal_id VARCHAR(50) UNIQUE,
          hubspot_contact_id VARCHAR(50),
          dealname VARCHAR(255),
          amount DECIMAL(12,2),
          pipeline_stage TINYINT,
          dealstage VARCHAR(100),
          source VARCHAR(100),
          createdate DATETIME,
          lastmodifieddate DATETIME,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          INDEX idx_hubspot_deal (hubspot_deal_id),
          INDEX idx_contact (hubspot_contact_id),
          INDEX idx_pipeline_stage (pipeline_stage),
          INDEX idx_created_date (createdate)
        )
      `;
      
      await connection.execute(createTableQuery);
      return true;
    } finally {
      await connection.end();
    }
  }

  /**
   * Insert or update a deal from HubSpot
   */
  async upsertDeal(dealData) {
    const connection = await this.getDbConnection();
    
    try {
      const query = `
        INSERT INTO hub_deals (
          hubspot_deal_id, hubspot_contact_id, dealname, amount, 
          pipeline_stage, dealstage, source, createdate, lastmodifieddate
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON DUPLICATE KEY UPDATE
        hubspot_contact_id = VALUES(hubspot_contact_id),
        dealname = VALUES(dealname),
        amount = VALUES(amount),
        pipeline_stage = VALUES(pipeline_stage),
        dealstage = VALUES(dealstage),
        lastmodifieddate = VALUES(lastmodifieddate),
        updated_at = CURRENT_TIMESTAMP
      `;
      
      const [result] = await connection.execute(query, [
        dealData.hubspotDealId,
        dealData.hubspotContactId || null,
        dealData.dealName,
        dealData.amount,
        dealData.pipelineStage,
        dealData.dealStage || null,
        dealData.source,
        dealData.createdAt,
        dealData.updatedAt
      ]);
      
      return result;
    } finally {
      await connection.end();
    }
  }

  /**
   * Get all deals
   */
  async getAllDeals() {
    const connection = await this.getDbConnection();
    
    try {
      const [rows] = await connection.execute(`
        SELECT * FROM hub_deals ORDER BY createdate DESC
      `);
      
      return rows;
    } finally {
      await connection.end();
    }
  }

  /**
   * Get deals by pipeline stage
   */
  async getDealsByStage(stageId) {
    const connection = await this.getDbConnection();
    
    try {
      const [rows] = await connection.execute(`
        SELECT * FROM hub_deals 
        WHERE pipeline_stage = ?
        ORDER BY createdate DESC
      `, [stageId]);
      
      return rows;
    } finally {
      await connection.end();
    }
  }

  /**
   * Get deal statistics
   */
  async getDealStats(days = 30) {
    const connection = await this.getDbConnection();
    
    try {
      const [rows] = await connection.execute(`
        SELECT 
          COUNT(*) as total_deals,
          COUNT(CASE WHEN createdate >= DATE_SUB(NOW(), INTERVAL ? DAY) THEN 1 END) as recent_deals,
          SUM(amount) as total_value,
          SUM(CASE WHEN createdate >= DATE_SUB(NOW(), INTERVAL ? DAY) THEN amount ELSE 0 END) as recent_value,
          AVG(amount) as average_deal_size,
          COUNT(CASE WHEN pipeline_stage = 7 THEN 1 END) as won_deals,
          SUM(CASE WHEN pipeline_stage = 7 THEN amount ELSE 0 END) as won_value
        FROM hub_deals
      `, [days, days]);
      
      return rows[0];
    } finally {
      await connection.end();
    }
  }

  /**
   * Get deals by contact
   */
  async getDealsByContact(hubspotContactId) {
    const connection = await this.getDbConnection();
    
    try {
      const [rows] = await connection.execute(`
        SELECT * FROM hub_deals 
        WHERE hubspot_contact_id = ?
        ORDER BY createdate DESC
      `, [hubspotContactId]);
      
      return rows;
    } finally {
      await connection.end();
    }
  }

  /**
   * Get database stats (for status dashboard)
   */
  async getDbStats() {
    const connection = await this.getDbConnection();
    
    try {
      const [rows] = await connection.execute(`
        SELECT 
          (SELECT COUNT(*) FROM hub_contacts) as total_contacts,
          (SELECT COUNT(*) FROM hub_deals) as total_deals,
          (SELECT COUNT(*) FROM country_rules) as total_countries,
          (SELECT COUNT(*) FROM pipeline_stages) as total_stages
      `);
      
      return rows[0];
    } finally {
      await connection.end();
    }
  }
}

module.exports = DealModel;