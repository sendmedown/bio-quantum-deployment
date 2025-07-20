from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
from flask_socketio import SocketIO, emit
import os
import json
import random
import time
from datetime import datetime, timedelta
import sqlite3
import threading

# Import models and routes
from models.user import User
from models.trading import MarketData, TradingSignal, Portfolio, KMNugget, QuantumMetrics
from routes.user import user_bp
from routes.trading import trading_bp
from websocket_handler import handle_websocket_events

app = Flask(__name__)
app.config['SECRET_KEY'] = 'bio-quantum-secret-key-2024'

# Enable CORS for all routes
CORS(app, origins="*")

# Initialize SocketIO with production settings
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

# Register blueprints
app.register_blueprint(user_bp, url_prefix='/api/users')
app.register_blueprint(trading_bp, url_prefix='/api/trading')

# Initialize database
def init_db():
    conn = sqlite3.connect('bio_quantum.db')
    cursor = conn.cursor()
    
    # Create tables
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            email TEXT UNIQUE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS market_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            price REAL NOT NULL,
            volume INTEGER NOT NULL,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS trading_signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            signal_type TEXT NOT NULL,
            confidence REAL NOT NULL,
            quantum_score REAL NOT NULL,
            bio_indicator REAL NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS portfolio (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            quantity REAL NOT NULL,
            avg_price REAL NOT NULL,
            current_value REAL NOT NULL,
            pnl REAL NOT NULL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS km_nuggets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            content TEXT NOT NULL,
            category TEXT NOT NULL,
            confidence REAL NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    conn.commit()
    conn.close()

# Serve static files (React frontend)
@app.route('/')
def serve_frontend():
    return send_from_directory('static', 'index.html')

@app.route('/<path:path>')
def serve_static_files(path):
    try:
        return send_from_directory('static', path)
    except:
        # If file not found, serve index.html for React routing
        return send_from_directory('static', 'index.html')

# Health check endpoint
@app.route('/health')
def health_check():
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'service': 'Bio-Quantum AI Trading Platform'
    })

# Register WebSocket event handlers
handle_websocket_events(socketio)

# Background task for real-time data simulation
def background_data_simulation():
    while True:
        try:
            # Simulate quantum metrics updates
            quantum_data = {
                'coherence': round(random.uniform(70, 85), 1),
                'entanglement': round(random.uniform(65, 80), 1),
                'superposition': round(random.uniform(75, 90), 1),
                'bio_sync': round(random.uniform(70, 85), 1),
                'quantum_advantage': round(random.uniform(1.2, 1.5), 2),
                'timestamp': datetime.now().isoformat()
            }
            
            socketio.emit('quantum_metrics_update', quantum_data)
            
            # Simulate market data updates
            symbols = ['BTC', 'ETH', 'AAPL', 'GOOGL', 'TSLA']
            for symbol in symbols:
                market_update = {
                    'symbol': symbol,
                    'price': round(random.uniform(100, 50000), 2),
                    'change': round(random.uniform(-5, 5), 2),
                    'volume': random.randint(1000000, 10000000),
                    'timestamp': datetime.now().isoformat()
                }
                socketio.emit('market_data_update', market_update)
            
            time.sleep(5)  # Update every 5 seconds
        except Exception as e:
            print(f"Background simulation error: {e}")
            time.sleep(10)

if __name__ == '__main__':
    # Initialize database
    init_db()
    
    # Start background simulation in a separate thread
    simulation_thread = threading.Thread(target=background_data_simulation, daemon=True)
    simulation_thread.start()
    
    # Get port from environment variable (Render sets this)
    port = int(os.environ.get('PORT', 5000))
    
    # Production-ready server configuration
    if os.environ.get('FLASK_ENV') == 'production':
        # Use production WSGI server
        socketio.run(app, host='0.0.0.0', port=port, debug=False, allow_unsafe_werkzeug=True)
    else:
        # Development mode
        socketio.run(app, host='0.0.0.0', port=port, debug=True)

