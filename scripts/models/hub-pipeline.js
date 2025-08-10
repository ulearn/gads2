/**
 * Pipeline Model - Data Access Layer
 * Centralizes all pipeline-related database operations
 * File: /home/hub/public_html/gads/scripts/models/hub-pipeline.js
 */
class PipelineModel {
  constructor(getDbConnection) {
    this.getDbConnection = getDbConnection;
  }

  /**
   * Update pipeline stages to match HubSpot
   */
  async updatePipelineStages() {
    const connection = await this.getDbConnection();
    
    try {
      // Clear existing stages
      await connection.execute('DELETE FROM pipeline_stages');
      
      // Insert HubSpot-mapped stages
      const stages = [
        [0, 'Inbox', 6, 'INBOX - Lead arrived, passed territory validation (SQL)', false],
        [1, 'Sequenced', 7, 'SEQUENCED - Added to email sequence', false],
        [2, 'Engaging', 8, 'ENGAGING - Opened or clicked email', false],
        [3, 'Responsive', 10, 'RESPONSIVE - Replied (key conversion step)', false],
        [4, 'Advising', 12, 'ADVISING - Active sales dialogue', false],
        [5, 'Negotiation', 14, 'CONSIDERATION & NEGOTIATION - Pricing discussion', false],
        [6, 'Contract', 16, 'CONTRACT - Contract sent', false],
        [7, 'Win', 20, 'WON - Converted to paying student', true],
        [8, 'Lost-Early', 0, 'Territory fail or immediate disqualification', true],
        [9, 'Lost-Pipeline', 0, 'Valid lead but did not convert', true]
      ];
      
      const insertQuery = `
        INSERT INTO pipeline_stages (stage_id, stage_name, stage_weight, description, is_terminal)
        VALUES (?, ?, ?, ?, ?)
      `;
      
      for (const stage of stages) {
        await connection.execute(insertQuery, stage);
      }
      
      return {
        success: true,
        stages_updated: stages.length
      };
      
    } finally {
      await connection.end();
    }
  }

  /**
   * Get all pipeline stages
   */
  async getAllStages() {
    const connection = await this.getDbConnection();
    
    try {
      const [rows] = await connection.execute(`
        SELECT * FROM pipeline_stages ORDER BY stage_id
      `);
      
      return rows;
    } finally {
      await connection.end();
    }
  }

  /**
   * Get HubSpot stage mapping
   */
  getHubSpotStageMapping() {
    return {
      'appointmentscheduled': 0,  // INBOX
      '113151423': 1,             // SEQUENCED
      'qualifiedtobuy': 2,        // ENGAGING
      '767120827': 3,             // RESPONSIVE
      'presentationscheduled': 4, // ADVISING
      'decisionmakerboughtin': 5, // NEGOTIATION
      'contractsent': 6,          // CONTRACT
      'closedwon': 7,             // WIN
      'closedlost': 9             // LOST-PIPELINE
    };
  }

  /**
   * Map HubSpot lifecycle stage to our pipeline stage
   */
  mapLifecycleStage(lifecycleStage) {
    const mapping = {
      'subscriber': 1,
      'lead': 2,
      'marketingqualifiedlead': 2,
      'salesqualifiedlead': 3,
      'opportunity': 4,
      'customer': 7
    };
    
    return mapping[lifecycleStage] || 0; // Default to Inbox
  }

  /**
   * Get pipeline health metrics
   */
  async getPipelineHealth(days = 30) {
    const connection = await this.getDbConnection();
    
    try {
      const [rows] = await connection.execute(`
        SELECT 
          COUNT(CASE WHEN pipeline_stage IN (0,1,2) THEN 1 END) as early_stage,
          COUNT(CASE WHEN pipeline_stage IN (3,4,5,6) THEN 1 END) as mid_stage,
          COUNT(CASE WHEN pipeline_stage = 7 THEN 1 END) as won_deals,
          COUNT(CASE WHEN pipeline_stage IN (8,9) THEN 1 END) as lost_deals
        FROM hub_contacts
        WHERE hubspot_created_at >= DATE_SUB(NOW(), INTERVAL ? DAY)
      `, [days]);
      
      return rows[0];
    } finally {
      await connection.end();
    }
  }

  /**
   * Get pipeline conversion funnel
   */
  async getPipelineConversions(days = 30) {
    const connection = await this.getDbConnection();
    
    try {
      const [rows] = await connection.execute(`
        SELECT 
          ps.stage_id,
          ps.stage_name,
          ps.stage_weight,
          COUNT(c.contact_id) as contact_count,
          COUNT(d.deal_id) as deal_count,
          SUM(d.amount) as total_value
        FROM pipeline_stages ps
        LEFT JOIN hub_contacts c ON ps.stage_id = c.pipeline_stage
          AND c.hubspot_created_at >= DATE_SUB(NOW(), INTERVAL ? DAY)
        LEFT JOIN hub_deals d ON c.hubspot_id = d.hubspot_contact_id
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
   * Get stage distribution
   */
  async getStageDistribution() {
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
}

module.exports = PipelineModel;