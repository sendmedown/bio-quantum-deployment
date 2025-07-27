require('dotenv').config();

const express = require('express');
const cors = require('cors');
const jwt = require('jsonwebtoken');
const { WebSocketServer } = require('ws');
const { v4: uuidv4 } = require('uuid');
const redis = require('redis');

const app = express();
app.use(cors());
app.use(express.json());

const dnaStrands = new Map();
global.wss = new WebSocketServer({ port: 5003 });

wss.on('connection', (ws, req) => {
  const token = req.url.split('token=')[1];
  try {
    jwt.verify(token, process.env.JWT_SECRET || 'dummy_jwt_secret_123');
    ws.sessionId = uuidv4();
  } catch (err) {
    ws.close();
  }
});

app.get('/health', (req, res) => {
  res.status(200).json({ status: 'ready' });
});

app.post('/nugget/create', (req, res) => {
  const { userId, content, promptId, context, type, origin, semanticIndex, temporalCluster, contextAttribution } = req.body;
  const token = req.headers.authorization?.split(' ')[1];
  try {
    jwt.verify(token, process.env.JWT_SECRET || 'dummy_jwt_secret_123');
    if (!userId || !content || !promptId || !context?.sessionId) {
      return res.status(400).json({ error: 'Missing required fields', requestId: uuidv4() });
    }
    const nuggetId = uuidv4();
    const sessionId = context.sessionId;
    const codon = {
      nuggetId,
      content,
      promptId,
      type: type || 'Condition',
      origin: origin || 'User',
      semanticIndex: semanticIndex || [],
      temporalCluster: temporalCluster || new Date().toISOString(),
      contextAttribution: contextAttribution || { userId, agentId: null },
      timestamp: new Date().toISOString()
    };
    const strand = dnaStrands.get(sessionId) || { sessionId, codons: [] };
    strand.codons.push(codon);
    dnaStrands.set(sessionId, strand);
    wss.clients.forEach(client => {
      if (client.sessionId === sessionId) {
        client.send(JSON.stringify({ type: 'nugget_update', ...codon, requestId: uuidv4() }));
      }
    });
    res.status(200).json({ status: 'success', nuggetId, sessionId, requestId: uuidv4() });
  } catch (err) {
    res.status(401).json({ error: 'Invalid JWT', requestId: uuidv4() });
  }
});

app.post('/nugget/query', async (req, res) => {
  const { filters } = req.body;
  const token = req.headers.authorization?.split(' ')[1];
  try {
    jwt.verify(token, process.env.JWT_SECRET || 'dummy_jwt_secret_123');
    const cacheKey = `nuggets:${JSON.stringify(filters)}`;
    const redisClient = redis.createClient({ url: process.env.REDIS_URL || 'redis://localhost:6379' });
    await redisClient.connect();
    try {
      const cached = await redisClient.get(cacheKey);
      if (cached) {
        await redisClient.quit();
        return res.status(200).json({ nuggets: JSON.parse(cached), requestId: uuidv4() });
      }
      const nuggets = Array.from(dnaStrands.values()).flatMap(s => s.codons).filter(n => {
        return (
          (!filters?.riskLevel || n.riskLevel === filters.riskLevel) &&
          (!filters?.agent || n.contextAttribution?.agentId === filters.agent) &&
          (!filters?.strategy || n.content.includes(filters.strategy))
        );
      });
      await redisClient.setEx(cacheKey, 3600, JSON.stringify(nuggets));
      await redisClient.quit();
      res.status(200).json({ nuggets, requestId: uuidv4() });
    } catch (err) {
      console.error('Redis error:', err);
      await redisClient.quit();
      const fallback = Array.from(dnaStrands.values()).flatMap(s => s.codons).filter(n => {
        return (
          (!filters?.riskLevel || n.riskLevel === filters.riskLevel) &&
          (!filters?.agent || n.contextAttribution?.agentId === filters.agent) &&
          (!filters?.strategy || n.content.includes(filters.strategy))
        );
      });
      res.status(200).json({ nuggets: fallback, requestId: uuidv4(), fallback: true });
    }
  } catch (err) {
    res.status(401).json({ error: 'Invalid JWT', requestId: uuidv4() });
  }
});

app.post('/nugget/outcome', (req, res) => {
  const { nuggetId, sessionId, outcome } = req.body;
  const token = req.headers.authorization?.split(' ')[1];
  try {
    jwt.verify(token, process.env.JWT_SECRET || 'dummy_jwt_secret_123');
    const strand = dnaStrands.get(sessionId);
    if (!strand) return res.status(404).json({ error: 'Session not found', requestId: uuidv4() });
    const codon = strand.codons.find(c => c.nuggetId === nuggetId);
    if (!codon) return res.status(404).json({ error: 'Nugget not found', requestId: uuidv4() });
    codon.outcome = outcome;
    dnaStrands.set(sessionId, strand);
    wss.clients.forEach(client => {
      if (client.sessionId === sessionId) {
        client.send(JSON.stringify({ type: 'nugget_update', nuggetId, outcome, requestId: uuidv4() }));
      }
    });
    res.status(200).json({ status: 'success', nuggetId, sessionId, requestId: uuidv4() });
  } catch (err) {
    res.status(401).json({ error: 'Invalid JWT', requestId: uuidv4() });
  }
});

app.get('/nugget/:id/timeline', (req, res) => {
  const { id } = req.params;
  const token = req.headers.authorization?.split(' ')[1];
  try {
    jwt.verify(token, process.env.JWT_SECRET || 'dummy_jwt_secret_123');
    const strands = Array.from(dnaStrands.values());
    const codon = strands.flatMap(s => s.codons).find(c => c.nuggetId === id);
    if (!codon) return res.status(404).json({ error: 'Nugget not found', requestId: uuidv4() });
    const timeline = [{
      event: 'Created',
      timestamp: codon.timestamp,
      details: { content: codon.content, type: codon.type, origin: codon.origin }
    }];
    if (codon.outcome) {
      timeline.push({
        event: 'Outcome',
        timestamp: new Date().toISOString(),
        details: codon.outcome
      });
    }
    res.status(200).json({ timeline, requestId: uuidv4() });
  } catch (err) {
    res.status(401).json({ error: 'Invalid JWT', requestId: uuidv4() });
  }
});

app.listen(10000, () => {
  console.log('🚀 API Bridge server listening on port 10000');
});