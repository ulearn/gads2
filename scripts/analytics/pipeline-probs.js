// ============================================================================
// File: /home/hub/public_html/gads/scripts/analytics/pipeline-probs.js
// Adds: Auto-resolve HubSpot internal stage IDs + accurate progression calc.
// ============================================================================

const DEFAULT_LIMIT = 50; // HubSpot history limit

const FRIENDLY_ORDER = [
  'inbox',
  'sequenced',
  'engaging',
  'responsive',
  'advising',
  'negotiation',
  'contact',
  'closedwon',
];

const DEFAULT_ALIASES = {
  inbox: ['inbox', 'sql', 'appointmentscheduled'],
  sequenced: ['sequenced'],
  engaging: ['engaging', 'presentationscheduled', 'email opened', 'clicked'],
  responsive: ['responsive', 'replied', 'decisionmakerboughtin'],
  advising: ['advising'],
  negotiation: ['negotiation', 'contractsent'],
  contact: ['contact', 'deposit', 'quote accepted', 'qualifiedtobuy'],
  closedwon: ['closedwon', 'closed won', 'won'],
};

function parseDateOnly(d) {
  if (!d) return null;
  if (d instanceof Date) return new Date(d.getFullYear(), d.getMonth(), d.getDate());
  const [y, m, day] = String(d).split('-').map(Number);
  if (!y || !m || !day) return null;
  return new Date(y, m - 1, day);
}

function getRangeFromQuery(query) {
  const now = new Date();
  
  // Handle days parameter (from dashboard)
  if (query.days) {
    const daysBack = parseInt(query.days);
    const start = new Date();
    start.setDate(start.getDate() - daysBack);
    return { start, end: now };
  }
  
  // Handle start/end parameters (from direct API calls)
  const startYTD = new Date(now.getFullYear(), 0, 1);
  const start = parseDateOnly(query.start) || startYTD;
  const end = parseDateOnly(query.end) || now;
  return { start, end };
}

async function resolveStages(hubspotClient, pipelineId, friendlyOrder = FRIENDLY_ORDER, aliasMap = DEFAULT_ALIASES) {
  const res = await hubspotClient.crm.pipelines.pipelinesApi.getAll('deals');
  const pipelines = res.results || [];
  const pipe = pipelineId ? pipelines.find(p => p.id === pipelineId) : pipelines[0];
  if (!pipe) throw new Error('No deal pipeline found for stage resolution');

  const stageList = (pipe.stages || []).map(s => ({ id: s.id, label: (s.label || '').toLowerCase() }));

  function findIdByAliases(aliases) {
    for (const a of aliases) {
      const needle = a.toLowerCase();
      const hit = stageList.find(s => s.label.includes(needle));
      if (hit) return hit.id;
    }
    return null;
  }

  const mapped = [];
  for (const key of friendlyOrder) {
    const aliases = aliasMap[key] || [key];
    const id = findIdByAliases(aliases);
    if (id) mapped.push(id);
  }

  const cwAliases = aliasMap.closedwon || ['closedwon'];
  const closedWonId = findIdByAliases(cwAliases);
  const stages = mapped.filter(s => s !== closedWonId);
  if (closedWonId) stages.push(closedWonId);

  return { stages, wonStage: closedWonId || stages[stages.length - 1] };
}

async function * dealsCohort(hubspotClient, { start, end, pipeline }) {
  let after;
  while (true) {
    let resp;
    resp = await hubspotClient.crm.deals.basicApi.getPage(
      DEFAULT_LIMIT,
      after,
      ['amount', 'dealstage', 'pipeline', 'hs_is_closed_won', 'createdate', 'closedate'],
      ['dealstage']
    );

    for (const d of resp.results) {
      const created = new Date(d.properties.createdate);
      if (created >= start && created <= end) {
        if (!pipeline || d.properties.pipeline === pipeline) yield d;
      }
    }
    if (!resp.paging?.next?.after) break;
    after = resp.paging.next.after;
  }
}

function computeProgression(stages, deals) {
  const progressedCounts = {};
  const reachedCounts = {};
  for (const s of stages) reachedCounts[s] = 0;

  for (const d of deals) {
    const hist = (d.propertiesWithHistory?.dealstage || [])
      .map(h => ({ v: h.value, t: new Date(h.timestamp).getTime() }))
      .sort((a, b) => a.t - b.t);

    const reachedSet = new Set(hist.map(h => h.v));
    for (const s of stages) if (reachedSet.has(s)) reachedCounts[s]++;

    for (let i = 0; i < stages.length - 1; i++) {
      const s = stages[i], s1 = stages[i + 1];
      const tsS = hist.find(h => h.v === s)?.t;
      const tsS1 = hist.find(h => h.v === s1)?.t;
      if (tsS != null && tsS1 != null && tsS1 > tsS) {
        const key = `${s}->${s1}`;
        progressedCounts[key] = (progressedCounts[key] || 0) + 1;
      }
    }
  }

  const progression = {};
  const stageLoss = {};
  for (let i = 0; i < stages.length - 1; i++) {
    const s = stages[i], s1 = stages[i + 1];
    const key = `${s}->${s1}`;
    const fromReached = reachedCounts[s] || 0;
    const progressed = progressedCounts[key] || 0;
    const p_advance = fromReached ? (progressed / fromReached) : 0;
    progression[key] = { from_reached: fromReached, progressed, p_advance };
    stageLoss[s] = 1 - p_advance;
  }
  return { progression, stageLoss, reachedCounts };
}

function computeStats({ deals, stages, wonStage, avgWonOverride }) {
  const reached = Object.create(null);
  const wonAfter = Object.create(null);
  let wonCount = 0, wonSum = 0;

  for (const d of deals) {
    const hist = d.propertiesWithHistory?.dealstage || [];
    if (!hist.length) continue;
    const ever = new Set(hist.map(h => h.value));
    for (const s of ever) reached[s] = (reached[s] || 0) + 1;
    const isWon = d.properties.hs_is_closed_won === 'true' || ever.has(wonStage);
    if (isWon) {
      for (const s of ever) wonAfter[s] = (wonAfter[s] || 0) + 1;
      wonCount++;
      const amt = parseFloat(d.properties.amount || '0') || 0;
      if (amt > 0) wonSum += amt;
    }
  }

  const avgWon = (typeof avgWonOverride === 'number') ? avgWonOverride : (wonCount ? wonSum / wonCount : 0);
  const completion = {};
  for (const s of stages) {
    const r = reached[s] || 0;
    const w = wonAfter[s] || 0;
    const pWin = r ? w / r : 0;
    completion[s] = { reached: r, wonAfter: w, p_win: pWin, euro_value: avgWon * pWin };
  }

  const { progression, stageLoss } = computeProgression(stages, deals);
  return { avgWon, reached, wonAfter, completion, progression, stageLoss };
}

async function handleGetProbabilities(req, res, hubspotClient) {
  try {
    const { start, end } = getRangeFromQuery(req.query);
    const pipeline = req.query.pipeline || null;
    const avgWonOverride = req.query.avgWon ? Number(req.query.avgWon) : undefined;

    let stages, wonStage;
    if (req.query.stages) {
      stages = JSON.parse(req.query.stages);
      wonStage = req.query.wonStage || stages[stages.length - 1];
    } else {
      const resolved = await resolveStages(hubspotClient, pipeline);
      stages = resolved.stages; wonStage = resolved.wonStage;
    }

    const deals = [];
    for await (const d of dealsCohort(hubspotClient, { start, end, pipeline })) deals.push(d);
    const stats = computeStats({ deals, stages, wonStage, avgWonOverride });

    res.json({
      success: true,
      cohort: { start: start.toISOString().slice(0,10), end: end.toISOString().slice(0,10), pipeline: pipeline || 'ALL' },
      config: { stages, wonStage },
      avgWon: stats.avgWon,
      completion: stats.completion,
      progression: stats.progression,
      stageLoss: stats.stageLoss,
      raw: { reached: stats.reached, wonAfter: stats.wonAfter },
      generatedAt: new Date().toISOString()
    });
  } catch (err) {
    console.error('❌ pipeline-probs auto-map error:', err);
    res.status(500).json({ success: false, error: err.message });
  }
}

module.exports = { handleGetProbabilities, computeStats, resolveStages };
