const express = require('express');
const { WebSocketServer } = require('ws');
const redis = require('redis');
const jwt = require('jsonwebtoken');
const { v4: uuidv4 } = require('uuid'); // Added for UUID generation
require('dotenv').config();

const app = express();
app.use(express.json());

// Validate environment variables
const requiredEnvVars = ['JWT_SECRET', 'PAYPAL_CLIENT_ID', 'PAYPAL_SECRET', 'ALPACA_API_KEY', 'ALPACA_SECRET_KEY'];
const missingEnvVars = requiredEnvVars.filter((envVar) => !process.env[envVar]);
if (missingEnvVars.length > 0) {
  console.error(`Missing environment variables: ${missingEnvVars.join(', ')}`);
  process.exit(1);
}

// Mock shared directory files (based on /shared)
const sharedFiles = [
  { fileName: 'hldd_integration_mapper.json', lastModified: '2025-07-17T03:00:00Z' },
  { fileName: 'bulk_import_pipeline.py', lastModified: '2025-07-17T03:00:00Z' },
  { fileName: 'notion_sync.py', lastModified: '2025-07-17T03:00:00Z' },
  { fileName: 'HLDD.txt', lastModified: '2025-07-17T03:00:00Z' }
];

// Mock HLDD structure (based on /hldd endpoint expectations)
const hlddStructure = {
  trade: {
    fields: ['asset', 'volume', 'price', 'timestamp'],
    source: 'bulk_import_pipeline.py',
    destination: 'dna_db_integration.py'
  },
  strategy: {
    fields: ['strategyId', 'params', 'creator'],
    source: 'photonic_gateway_rl.py',
    destination: 'strategy_replay.js'
  },
  chat: {
    fields: ['userId', 'message', 'timestamp'],
    source: 'notion_sync.py',
    destination: 'apiBridge.js'
  }
};

// Express Endpoints
app.get('/health', (req, res) => {
  res.status(200).json({ status: 'success', message: 'apiBridge.js is running on Render', requestId: uuidv4() });
});

app.get('/hldd', (req, res) => {
  res.status(200).json({ ...hlddStructure, requestId: uuidv4() });
});

app.get('/get-file/:fileName', (req, res) => {
  const { fileName } = req.params;
  const file = sharedFiles.find((f) => f.fileName === fileName);
  if (file) {
    res.status(200).json({
      fileName,
      content: `Mock content for ${fileName}`,
      fileType: fileName.split('.').pop(),
      requestId: uuidv4()
    });
  } else {
    res.status(404).json({ error: 'File not found', requestId: uuidv4() });
  }
});

app.get('/list-files', (req, res) => {
  res.status(200).json({ files: sharedFiles, requestId: uuidv4() });
});

// WebSocket Setup
const PORT = process.env.PORT || 10000;
const server = app.listen(PORT, '0.0.0.0', () => {
  console.log(`apiBridge.js running on port ${PORT}`);
});

const wss = new WebSocketServer({ server });

wss.on('connection', (ws, req) => {
  // Extract JWT from query string (e.g., wss://bioquantum-api.onrender.com:5003?token=<jwt>)
  const urlParams = new URLSearchParams(req.url.split('?')[1]);
  const token = urlParams.get('token');

  // Validate JWT
  try {
    const decoded = jwt.verify(token, process.env.JWT_SECRET || 'dummy_jwt_secret_123');
    ws.userId = decoded.userId;
  } catch (err) {
    ws.send(JSON.stringify({ error: 'Invalid or missing JWT', requestId: uuidv4() }));
    ws.close();
    return;
  }

  ws.on('message', (data) => {
    try {
      const message = JSON.parse(data);
      if (message.type === 'share_file') {
        sharedFiles.push({
          fileName: message.fileName,
          lastModified: new Date().toISOString()
        });
        ws.send(
          JSON.stringify({
            type: 'file_shared',
            fileName: message.fileName,
            fileType: message.fileType,
            requestId: uuidv4()
          })
        );
      }
    } catch (err) {
      ws.send(JSON.stringify({ error: 'Invalid message format', requestId: uuidv4() }));
    }
  });

  ws.on('close', () => {
    console.log(`WebSocket connection closed for user: ${ws.userId}`);
  });
});