# Bio-Quantum AI Trading Platform

A comprehensive trading platform that combines quantum computing principles with biological pattern recognition for advanced market analysis and trading signal generation.

## 🚀 Features

### Backend (Flask API)
- **RESTful API** with comprehensive trading endpoints
- **WebSocket Support** for real-time market data streaming
- **Quantum Metrics Analysis** with coherence and entanglement calculations
- **Bio-Sentiment Indicators** for market psychology analysis
- **Knowledge Management (KM) Nuggets** for AI-curated insights
- **Portfolio Management** with P&L tracking
- **Trading Signals** with confidence scoring
- **SQLite Database** for data persistence
- **CORS Enabled** for cross-origin requests

### Frontend (React + Vite)
- **Modern React 18** with functional components and hooks
- **Tailwind CSS** for responsive design
- **shadcn/ui Components** for professional UI elements
- **Recharts** for advanced data visualization
- **Real-time WebSocket** integration
- **Multi-page Navigation** with React Router
- **Responsive Design** for desktop and mobile
- **Dark Theme** with purple/cyan gradient aesthetics

### Key Pages
1. **Dashboard** - Real-time quantum metrics, market overview, price charts
2. **Portfolio** - Holdings tracking, P&L analysis, distribution charts
3. **Signals** - AI-powered trading recommendations with filtering
4. **Knowledge** - KM nuggets with categorization and search
5. **Analytics** - Advanced performance metrics and quantum analysis

## 📁 Project Structure

```
bio-quantum-trading-platform/
├── backend/
│   └── bio_quantum_api/
│       ├── src/
│       │   ├── models/
│       │   │   ├── user.py
│       │   │   └── trading.py
│       │   ├── routes/
│       │   │   ├── user.py
│       │   │   └── trading.py
│       │   ├── static/          # Frontend build files go here
│       │   ├── main.py          # Flask application entry point
│       │   └── websocket_handler.py
│       ├── venv/                # Python virtual environment
│       └── requirements.txt
└── frontend/
    └── bio-quantum-ui/
        ├── src/
        │   ├── components/
        │   │   ├── ui/          # shadcn/ui components
        │   │   ├── Dashboard.jsx
        │   │   ├── Portfolio.jsx
        │   │   ├── Signals.jsx
        │   │   ├── KMNuggets.jsx
        │   │   ├── Analytics.jsx
        │   │   └── Navigation.jsx
        │   ├── hooks/
        │   │   └── useWebSocket.jsx
        │   ├── App.jsx
        │   └── main.jsx
        ├── package.json
        └── vite.config.js
```

## 🛠️ Installation & Setup

### Prerequisites
- Python 3.11+
- Node.js 20+
- pnpm (recommended) or npm

### Backend Setup

1. Navigate to backend directory:
```bash
cd backend/bio_quantum_api
```

2. Create and activate virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Start the Flask server:
```bash
python src/main.py
```

The backend will be available at `http://localhost:5000`

### Frontend Setup

1. Navigate to frontend directory:
```bash
cd frontend/bio-quantum-ui
```

2. Install dependencies:
```bash
pnpm install  # or npm install
```

3. Start development server:
```bash
pnpm run dev --host  # or npm run dev -- --host
```

The frontend will be available at `http://localhost:5173`

## 🚀 Deployment

### For Production Deployment

1. **Build Frontend:**
```bash
cd frontend/bio-quantum-ui
pnpm run build
```

2. **Copy Build to Backend:**
```bash
cp -r dist/* ../../backend/bio_quantum_api/src/static/
```

3. **Deploy Backend:**
The Flask app serves both API and frontend from a single server.

### Render Deployment

1. **Backend (Web Service):**
   - Build Command: `cd backend/bio_quantum_api && pip install -r requirements.txt`
   - Start Command: `cd backend/bio_quantum_api && python src/main.py`
   - Environment: Python 3.11

2. **Environment Variables:**
   - `FLASK_ENV=production`
   - `PORT=5000` (Render will override this)

### GitHub Repository Setup

1. Initialize git repository:
```bash
git init
git add .
git commit -m "Initial commit: Bio-Quantum AI Trading Platform"
```

2. Add remote and push:
```bash
git remote add origin <your-github-repo-url>
git push -u origin main
```

## 📊 API Endpoints

### Market Data
- `GET /api/trading/market-data` - Get all market data
- `GET /api/trading/market-data/<symbol>` - Get specific symbol data

### Trading Signals
- `GET /api/trading/signals` - Get active trading signals
- `POST /api/trading/signals` - Create new trading signal

### Portfolio
- `GET /api/trading/portfolio/<user_id>` - Get user portfolio
- `POST /api/trading/portfolio` - Update portfolio position

### KM Nuggets
- `GET /api/trading/km-nuggets` - Get knowledge nuggets
- `POST /api/trading/km-nuggets` - Create new nugget

### Analytics
- `GET /api/trading/analytics/performance` - Get performance metrics
- `GET /api/trading/analytics/quantum-metrics` - Get quantum analysis

### Demo Data
- `POST /api/trading/demo/generate-data` - Generate sample data (clearly labeled as simulation)

## 🔌 WebSocket Events

### Client → Server
- `subscribe_market_data` - Subscribe to real-time market updates
- `subscribe_signals` - Subscribe to trading signal updates
- `subscribe_quantum_metrics` - Subscribe to quantum metrics updates

### Server → Client
- `market_data_update` - Real-time price and volume updates
- `trading_signal_update` - New trading signal notifications
- `quantum_metrics_update` - Quantum analysis updates
- `platform_alert` - System alerts and notifications

## 🧪 Testing

### Backend Testing
```bash
cd backend/bio_quantum_api
source venv/bin/activate
python -m pytest  # If tests are added
```

### Frontend Testing
```bash
cd frontend/bio-quantum-ui
pnpm test  # or npm test
```

## 🔧 Configuration

### Backend Configuration
- Database: SQLite (configurable in `main.py`)
- CORS: Enabled for all origins
- WebSocket: Enabled with threading support
- Debug: Enabled in development

### Frontend Configuration
- Vite: Hot reload enabled
- Tailwind: Configured with custom theme
- API Base URL: Configurable in environment variables

## 📈 Quantum Metrics Explained

### Core Metrics
- **Quantum Coherence**: Stability of quantum states in market analysis
- **Entanglement Strength**: Correlation between market variables
- **Superposition Stability**: Ability to maintain multiple market scenarios
- **Bio-Quantum Sync**: Alignment between biological patterns and quantum analysis
- **Quantum Advantage**: Performance multiplier from quantum processing

### Bio-Sentiment Indicators
- **Bio Indicator**: Biological pattern recognition score
- **Market Psychology**: Sentiment analysis based on bio-rhythmic patterns
- **Emotional Volatility**: Market emotion fluctuation measurements

## ⚠️ Important Notes

### Simulation Disclaimer
- All trading signals and market data include clear labeling as "SIMULATED" or "Demo purposes only"
- This platform is for educational and demonstration purposes
- Real trading integration requires additional compliance and risk management features

### Security Considerations
- Change default secret keys before production deployment
- Implement proper authentication for production use
- Add rate limiting for API endpoints
- Use environment variables for sensitive configuration

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## 📄 License

This project is provided as-is for educational and demonstration purposes.

## 🆘 Support

For issues and questions:
1. Check the GitHub Issues page
2. Review the API documentation
3. Test with the demo data generation endpoints

---

**Built with ❤️ using Flask, React, and Quantum-Inspired Algorithms**

