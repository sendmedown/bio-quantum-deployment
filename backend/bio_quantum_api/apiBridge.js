require('dotenv').config();
const express = require('express');
const cors = require('cors');
const jwt = require('jsonwebtoken');
const { WebSocketServer } = require('ws');
const { v4: uuidv4 } = require('uuid');
const redis = require('redis');
const multer = require('multer');
const fs = require('fs');
const path = require('path');
const { graphqlHTTP } = require('express-graphql');
const { buildSchema } = require('graphql');

const app = express();
app.use(cors());
app.use(express.json());

// Setup logging
const logFile = path.join(__dirname, 'apiBridge.log');
function log(message, level = 'INFO') {
  const timestamp = new Date().toISOString();
  const logMessage = `[${timestamp}] ${level}: ${message}\n`;
  fs.appendFileSync(logFile, logMessage);
}
log('Starting API Bridge server...');

// Persistent storage
const dnaStrands = new Map();

// WebSocket setup
global.wss = new WebSocketServer({ port: 5003 });
wss.on('connection', (ws, req) => {
  const token = req.url.split('token=')[1];
  try {
    jwt.verify(token, process.env.JWT_SECRET || 'dummy_jwt_secret_123');
    ws.sessionId = uuidv4();
    log(`WebSocket connected, sessionId: ${ws.sessionId}`);
  } catch (err) {
    ws.close();
    log(`WebSocket connection failed: ${err.message}`, 'ERROR');
  }
});

// File upload setup
const uploadDir = path.join(__dirname, 'shared');
if (!fs.existsSync(uploadDir)) {
  fs.mkdirSync(uploadDir);
  log(`Created shared directory: ${uploadDir}`);
}
const storage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, uploadDir),
  filename: (req, file, cb) => cb(null, file.originalname)
});
const upload = multer({ storage });

// Health endpoint
app.get('/health', (req, res) => {
  try {
    res.status(200).json({ status: 'ready' });
    log('Health check successful');
  } catch (err) {
    res.status(500).json({ error: 'Health check failed', requestId: uuidv4() });
    log(`Health check failed: ${err.message}`, 'ERROR');
  }
});

// Nugget create endpoint
app.post('/nugget/create', (req, res) => {
  const { userId, content, promptId, context, type, origin, semanticIndex, temporalCluster, contextAttribution } = req.body;
  const token = req.headers.authorization?.split(' ')[1];
  try {
    jwt.verify(token, process.env.JWT_SECRET || 'dummy_jwt_secret_123');
    if (!userId || !content || !promptId || !context?.sessionId) {
      throw new Error('Missing required fields');
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
    log(`Nugget created: ${nuggetId}, session: ${sessionId}`);
  } catch (err) {
    res.status(401).json({ error: err.message || 'Invalid JWT', requestId: uuidv4() });
    log(`Nugget create failed: ${err.message}`, 'ERROR');
  }
});

// Nugget query endpoint
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
        res.status(200).json({ nuggets: JSON.parse(cached), requestId: uuidv4() });
        log(`Nugget query served from cache: ${cacheKey}`);
        return;
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
      log(`Nugget query executed: ${cacheKey}`);
    } catch (err) {
      log(`Redis error: ${err.message}`, 'ERROR');
      await redisClient.quit();
      const fallback = Array.from(dnaStrands.values()).flatMap(s => s.codons).filter(n => {
        return (
          (!filters?.riskLevel || n.riskLevel === filters.riskLevel) &&
          (!filters?.agent || n.contextAttribution?.agentId === filters.agent) &&
          (!filters?.strategy || n.content.includes(filters.strategy))
        );
      });
      res.status(200).json({ nuggets: fallback, requestId: uuidv4(), fallback: true });
      log(`Nugget query fallback: ${cacheKey}`);
    }
  } catch (err) {
    res.status(401).json({ error: 'Invalid JWT', requestId: uuidv4() });
    log(`Nugget query failed: ${err.message}`, 'ERROR');
  }
});

// Nugget outcome endpoint
app.post('/nugget/outcome', (req, res) => {
  const { nuggetId, sessionId, outcome } = req.body;
  const token = req.headers.authorization?.split(' ')[1];
  try {
    jwt.verify(token, process.env.JWT_SECRET || 'dummy_jwt_secret_123');
    const strand = dnaStrands.get(sessionId);
    if (!strand) {
      throw new Error('Session not found');
    }
    const codon = strand.codons.find(c => c.nuggetId === nuggetId);
    if (!codon) {
      throw new Error('Nugget not found');
    }
    codon.outcome = outcome;
    dnaStrands.set(sessionId, strand);
    wss.clients.forEach(client => {
      if (client.sessionId === sessionId) {
        client.send(JSON.stringify({ type: 'nugget_update', nuggetId, outcome, requestId: uuidv4() }));
      }
    });
    res.status(200).json({ status: 'success', nuggetId, sessionId, requestId: uuidv4() });
    log(`Nugget outcome updated: ${nuggetId}, session: ${sessionId}`);
  } catch (err) {
    res.status(401).json({ error: err.message || 'Invalid JWT', requestId: uuidv4() });
    log(`Nugget outcome failed: ${err.message}`, 'ERROR');
  }
});

// Nugget timeline endpoint
app.get('/nugget/:id/timeline', (req, res) => {
  const { id } = req.params;
  const token = req.headers.authorization?.split(' ')[1];
  try {
    jwt.verify(token, process.env.JWT_SECRET || 'dummy_jwt_secret_123');
    const strands = Array.from(dnaStrands.values());
    const codon = strands.flatMap(s => s.codons).find(c => c.nuggetId === id);
    if (!codon) {
      throw new Error('Nugget not found');
    }
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
    log(`Nugget timeline retrieved: ${id}`);
  } catch (err) {
    res.status(401).json({ error: err.message || 'Invalid JWT', requestId: uuidv4() });
    log(`Nugget timeline failed: ${err.message}`, 'ERROR');
  }
});

// Shared file endpoints
app.post('/shared/upload', upload.single('file'), (req, res) => {
  const token = req.headers.authorization?.split(' ')[1];
  try {
    jwt.verify(token, process.env.JWT_SECRET || 'dummy_jwt_secret_123');
    res.status(200).json({ status: 'success', filename: req.file.filename });
    log(`File uploaded: ${req.file.filename}`);
  } catch (err) {
    res.status(401).json({ error: 'Invalid JWT', requestId: uuidv4() });
    log(`File upload failed: ${err.message}`, 'ERROR');
  }
});

app.get('/shared/:filename', (req, res) => {
  const token = req.headers.authorization?.split(' ')[1];
  try {
    jwt.verify(token, process.env.JWT_SECRET || 'dummy_jwt_secret_123');
    const filePath = path.join(uploadDir, req.params.filename);
    if (fs.existsSync(filePath)) {
      res.download(filePath);
      log(`File downloaded: ${req.params.filename}`);
    } else {
      res.status(404).json({ error: 'File not found', requestId: uuidv4() });
      log(`File download failed: File not found - ${req.params.filename}`, 'ERROR');
    }
  } catch (err) {
    res.status(401).json({ error: 'Invalid JWT', requestId: uuidv4() });
    log(`File download failed: ${err.message}`, 'ERROR');
  }
});

// GraphQL endpoint
const schema = buildSchema(`
  type Nugget {
    nuggetId: String!
    content: String!
    promptId: String!
    type: String
    origin: String
    semanticIndex: [String]
    temporalCluster: String
    contextAttribution: ContextAttribution
    timestamp: String
  }
  type ContextAttribution {
    userId: String
    agentId: String
  }
  type Query {
    nugget(id: String!): Nugget
    nuggets(filters: NuggetFilter): [Nugget]
  }
  input NuggetFilter {
    riskLevel: String
    agent: String
    strategy: String
  }
`);

const root = {
  nugget: ({ id }) => {
    try {
      const nugget = Array.from(dnaStrands.values())
        .flatMap(s => s.codons)
        .find(c => c.nuggetId === id);
      if (!nugget) throw new Error('Nugget not found');
      log(`GraphQL query: nugget ${id}`);
      return nugget;
    } catch (err) {
      log(`GraphQL query failed: ${err.message}`, 'ERROR');
      throw err;
    }
  },
  nuggets: ({ filters }) => {
    try {
      const nuggets = Array.from(dnaStrands.values())
        .flatMap(s => s.codons)
        .filter(n =>
          (!filters?.riskLevel || n.riskLevel === filters.riskLevel) &&
          (!filters?.agent || n.contextAttribution?.agentId === filters.agent) &&
          (!filters?.strategy || n.content.includes(filters.strategy))
        );
      log(`GraphQL query: nuggets with filters ${JSON.stringify(filters)}`);
      return nuggets;
    } catch (err) {
      log(`GraphQL query failed: ${err.message}`, 'ERROR');
      throw err;
    }
  }
};

app.use('/graphql', (req, res, next) => {
  const token = req.headers.authorization?.split(' ')[1];
  try {
    jwt.verify(token, process.env.JWT_SECRET || 'dummy_jwt_secret_123');
    next();
    log('GraphQL auth successful');
  } catch (err) {
    res.status(401).json({ error: 'Invalid JWT', requestId: uuidv4() });
    log(`GraphQL auth failed: ${err.message}`, 'ERROR');
  }
}, graphqlHTTP({
  schema,
  rootValue: root,
  graphiql: true
}));

app.listen(10000, () => {
  console.log('🚀 API Bridge server listening on port 10000');
  log('API Bridge server started on port 10000');
});