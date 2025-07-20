import json
import threading
import time
import random
from datetime import datetime
from flask import request
from flask_socketio import SocketIO, emit, join_room, leave_room
from src.models.trading import MarketData, TradingSignal

class WebSocketHandler:
    def __init__(self, app=None):
        self.socketio = None
        self.active_connections = set()
        self.market_data_thread = None
        self.running = False
        
        if app:
            self.init_app(app)
    
    def init_app(self, app):
        self.socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')
        self.setup_handlers()
    
    def setup_handlers(self):
        @self.socketio.on('connect')
        def handle_connect():
            print(f'Client connected: {request.sid}')
            self.active_connections.add(request.sid)
            emit('connection_status', {'status': 'connected', 'message': 'Connected to Bio-Quantum Trading Platform'})
        
        @self.socketio.on('disconnect')
        def handle_disconnect():
            print(f'Client disconnected: {request.sid}')
            self.active_connections.discard(request.sid)
        
        @self.socketio.on('subscribe_market_data')
        def handle_subscribe_market_data(data):
            symbols = data.get('symbols', [])
            join_room('market_data')
            emit('subscription_status', {
                'type': 'market_data',
                'symbols': symbols,
                'status': 'subscribed'
            })
            
            # Start market data streaming if not already running
            if not self.running:
                self.start_market_data_stream()
        
        @self.socketio.on('subscribe_signals')
        def handle_subscribe_signals():
            join_room('trading_signals')
            emit('subscription_status', {
                'type': 'trading_signals',
                'status': 'subscribed'
            })
        
        @self.socketio.on('subscribe_quantum_metrics')
        def handle_subscribe_quantum_metrics():
            join_room('quantum_metrics')
            emit('subscription_status', {
                'type': 'quantum_metrics',
                'status': 'subscribed'
            })
        
        @self.socketio.on('unsubscribe')
        def handle_unsubscribe(data):
            subscription_type = data.get('type')
            if subscription_type == 'market_data':
                leave_room('market_data')
            elif subscription_type == 'trading_signals':
                leave_room('trading_signals')
            elif subscription_type == 'quantum_metrics':
                leave_room('quantum_metrics')
            
            emit('subscription_status', {
                'type': subscription_type,
                'status': 'unsubscribed'
            })
    
    def start_market_data_stream(self):
        """Start the market data streaming thread"""
        if self.market_data_thread is None or not self.market_data_thread.is_alive():
            self.running = True
            self.market_data_thread = threading.Thread(target=self._market_data_worker)
            self.market_data_thread.daemon = True
            self.market_data_thread.start()
    
    def stop_market_data_stream(self):
        """Stop the market data streaming"""
        self.running = False
        if self.market_data_thread:
            self.market_data_thread.join(timeout=1)
    
    def _market_data_worker(self):
        """Background worker for streaming market data"""
        symbols = ['BTC', 'ETH', 'AAPL', 'GOOGL', 'TSLA', 'NVDA', 'AMZN', 'MSFT']
        
        while self.running and self.active_connections:
            try:
                # Generate real-time market data updates
                for symbol in symbols:
                    if not self.running:
                        break
                    
                    # Simulate price movement
                    base_price = random.uniform(100, 50000)
                    price_change = random.uniform(-0.05, 0.05)  # ±5% change
                    new_price = base_price * (1 + price_change)
                    
                    market_update = {
                        'symbol': symbol,
                        'price': round(new_price, 2),
                        'change_24h': round(base_price * price_change, 2),
                        'change_percent_24h': round(price_change * 100, 2),
                        'volume': random.uniform(1000000, 100000000),
                        'quantum_volatility': round(random.uniform(0.1, 1.0), 3),
                        'bio_sentiment': round(random.uniform(0.1, 1.0), 3),
                        'timestamp': datetime.utcnow().isoformat()
                    }
                    
                    # Emit to subscribed clients
                    self.socketio.emit('market_data_update', market_update, room='market_data')
                
                # Generate quantum metrics updates
                quantum_metrics = {
                    'quantum_coherence': round(random.uniform(0.7, 0.95), 3),
                    'entanglement_strength': round(random.uniform(0.6, 0.9), 3),
                    'superposition_stability': round(random.uniform(0.8, 0.98), 3),
                    'decoherence_rate': round(random.uniform(0.01, 0.05), 4),
                    'quantum_advantage': round(random.uniform(1.2, 2.5), 2),
                    'bio_quantum_sync': round(random.uniform(0.75, 0.92), 3),
                    'timestamp': datetime.utcnow().isoformat()
                }
                
                self.socketio.emit('quantum_metrics_update', quantum_metrics, room='quantum_metrics')
                
                # Occasionally generate trading signals
                if random.random() < 0.3:  # 30% chance
                    signal = {
                        'symbol': random.choice(symbols),
                        'signal_type': random.choice(['BUY', 'SELL', 'HOLD']),
                        'confidence': round(random.uniform(0.6, 0.95), 2),
                        'price': round(random.uniform(100, 50000), 2),
                        'quantum_score': round(random.uniform(0.5, 1.0), 3),
                        'bio_indicator': round(random.uniform(0.5, 1.0), 3),
                        'timestamp': datetime.utcnow().isoformat(),
                        'note': 'SIMULATED SIGNAL - Demo purposes only'
                    }
                    
                    self.socketio.emit('trading_signal_update', signal, room='trading_signals')
                
                # Wait before next update
                time.sleep(2)  # Update every 2 seconds
                
            except Exception as e:
                print(f"Error in market data worker: {e}")
                time.sleep(5)  # Wait longer on error
        
        print("Market data worker stopped")
    
    def broadcast_alert(self, alert_type, message, data=None):
        """Broadcast an alert to all connected clients"""
        alert = {
            'type': alert_type,
            'message': message,
            'data': data,
            'timestamp': datetime.utcnow().isoformat()
        }
        
        self.socketio.emit('platform_alert', alert)
    
    def get_socketio(self):
        """Get the SocketIO instance"""
        return self.socketio

# Global instance
websocket_handler = WebSocketHandler()

