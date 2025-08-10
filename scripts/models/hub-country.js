/**
 * Country Model - Data Access Layer
 * Centralizes all country/territory-related database operations
 * File: /home/hub/public_html/gads/scripts/models/hub-country.js
 */
class CountryModel {
  constructor(getDbConnection) {
    this.getDbConnection = getDbConnection;
  }

  /**
   * Get country code from country name
   */
  async getCountryCode(countryName) {
    if (!countryName) return null;
    
    const connection = await this.getDbConnection();
    
    try {
      const [rows] = await connection.execute(
        'SELECT country_code FROM country_rules WHERE country_name = ? LIMIT 1',
        [countryName]
      );
      
      return rows.length > 0 ? rows[0].country_code : null;
    } catch (error) {
      return null;
    } finally {
      await connection.end();
    }
  }

  /**
   * Get all country rules
   */
  async getAllCountryRules() {
    const connection = await this.getDbConnection();
    
    try {
      const [rows] = await connection.execute(`
        SELECT * FROM country_rules ORDER BY country_name
      `);
      
      return rows;
    } finally {
      await connection.end();
    }
  }

  /**
   * Get territory health overview
   */
  async getTerritoryHealth() {
    const connection = await this.getDbConnection();
    
    try {
      const [rows] = await connection.execute(`
        SELECT 
          COUNT(CASE WHEN cr.status = 'green' THEN 1 END) as green_countries,
          COUNT(CASE WHEN cr.status = 'yellow' THEN 1 END) as yellow_countries,
          COUNT(CASE WHEN cr.status = 'red' THEN 1 END) as red_countries,
          COUNT(DISTINCT c.country_code) as active_countries
        FROM country_rules cr
        LEFT JOIN hub_contacts c ON cr.country_code = c.country_code
      `);
      
      return rows[0];
    } finally {
      await connection.end();
    }
  }

  /**
   * Get country performance metrics
   */
  async getCountryPerformance(days = 30) {
    const connection = await this.getDbConnection();
    
    try {
      const [rows] = await connection.execute(`
        SELECT 
          cr.country_name,
          cr.status,
          cr.country_code,
          COUNT(c.contact_id) as total_contacts,
          COUNT(CASE WHEN c.pipeline_stage >= 3 THEN 1 END) as responsive_contacts,
          COUNT(CASE WHEN c.pipeline_stage = 7 THEN 1 END) as won_contacts,
          ROUND(COUNT(CASE WHEN c.pipeline_stage >= 3 THEN 1 END) * 100.0 / NULLIF(COUNT(c.contact_id), 0), 2) as responsive_rate,
          ROUND(COUNT(CASE WHEN c.pipeline_stage = 7 THEN 1 END) * 100.0 / NULLIF(COUNT(c.contact_id), 0), 2) as win_rate
        FROM country_rules cr
        LEFT JOIN hub_contacts c ON cr.country_code = c.country_code 
          AND c.hubspot_created_at >= DATE_SUB(CURDATE(), INTERVAL ? DAY)
        WHERE c.contact_id IS NOT NULL
        GROUP BY cr.country_code, cr.country_name, cr.status
        ORDER BY total_contacts DESC
      `, [days]);
      
      return rows;
    } finally {
      await connection.end();
    }
  }

  /**
   * Update country status (for territory management)
   */
  async updateCountryStatus(countryCode, status) {
    const connection = await this.getDbConnection();
    
    try {
      const [result] = await connection.execute(
        'UPDATE country_rules SET status = ? WHERE country_code = ?',
        [status, countryCode]
      );
      
      return result.affectedRows > 0;
    } finally {
      await connection.end();
    }
  }

  /**
   * Add new country rule
   */
  async addCountryRule(countryData) {
    const connection = await this.getDbConnection();
    
    try {
      const [result] = await connection.execute(`
        INSERT INTO country_rules (country_code, country_name, status, notes)
        VALUES (?, ?, ?, ?)
        ON DUPLICATE KEY UPDATE
        country_name = VALUES(country_name),
        status = VALUES(status),
        notes = VALUES(notes)
      `, [
        countryData.countryCode,
        countryData.countryName,
        countryData.status || 'yellow',
        countryData.notes || null
      ]);
      
      return result;
    } finally {
      await connection.end();
    }
  }

  /**
   * Get territory burn rate
   */
  async getTerritoryBurnRate(days = 30) {
    const connection = await this.getDbConnection();
    
    try {
      const [rows] = await connection.execute(`
        SELECT 
          cr.status,
          COUNT(c.contact_id) as contact_count,
          SUM(CASE WHEN d.deal_id IS NOT NULL THEN 1 ELSE 0 END) as deals_count,
          ROUND(AVG(d.amount), 2) as avg_deal_value
        FROM country_rules cr
        LEFT JOIN hub_contacts c ON cr.country_code = c.country_code
          AND c.hubspot_created_at >= DATE_SUB(CURDATE(), INTERVAL ? DAY)
        LEFT JOIN hub_deals d ON c.hubspot_id = d.hubspot_contact_id
        GROUP BY cr.status
        ORDER BY cr.status
      `, [days]);
      
      return rows;
    } finally {
      await connection.end();
    }
  }
}

module.exports = CountryModel;