from flask import Blueprint, request, jsonify
from src.models.trading import db, TradingSignal, Portfolio, KMNugget, MarketData
from src.models.user import User
import random
import json
from datetime import datetime, timedelta

trading_bp = Blueprint('trading', __name__)

# Market Data Endpoints
@trading_bp.route('/market-data', methods=['GET'])
def get_market_data():
    """Get current market data for all symbols"""
    try:
        market_data = MarketData.query.order_by(MarketData.timestamp.desc()).limit(50).all()
        return jsonify({
            'success': True,
            'data': [data.to_dict() for data in market_data]
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@trading_bp.route('/market-data/<symbol>', methods=['GET'])
def get_symbol_data(symbol):
    """Get market data for a specific symbol"""
    try:
        data = MarketData.query.filter_by(symbol=symbol.upper()).order_by(MarketData.timestamp.desc()).first()
        if not data:
            return jsonify({'success': False, 'error': 'Symbol not found'}), 404
        
        return jsonify({
            'success': True,
            'data': data.to_dict()
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# Trading Signals Endpoints
@trading_bp.route('/signals', methods=['GET'])
def get_trading_signals():
    """Get active trading signals"""
    try:
        signals = TradingSignal.query.filter_by(is_active=True).order_by(TradingSignal.timestamp.desc()).limit(20).all()
        return jsonify({
            'success': True,
            'data': [signal.to_dict() for signal in signals]
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@trading_bp.route('/signals', methods=['POST'])
def create_trading_signal():
    """Create a new trading signal"""
    try:
        data = request.get_json()
        
        signal = TradingSignal(
            symbol=data['symbol'].upper(),
            signal_type=data['signal_type'].upper(),
            confidence=data['confidence'],
            price=data['price'],
            quantum_score=data.get('quantum_score', random.uniform(0.1, 1.0)),
            bio_indicator=data.get('bio_indicator', random.uniform(0.1, 1.0))
        )
        
        db.session.add(signal)
        db.session.commit()
        
        return jsonify({
            'success': True,
            'data': signal.to_dict()
        }), 201
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# Portfolio Endpoints
@trading_bp.route('/portfolio/<int:user_id>', methods=['GET'])
def get_portfolio(user_id):
    """Get user portfolio"""
    try:
        portfolio = Portfolio.query.filter_by(user_id=user_id).all()
        return jsonify({
            'success': True,
            'data': [item.to_dict() for item in portfolio]
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@trading_bp.route('/portfolio', methods=['POST'])
def update_portfolio():
    """Update portfolio position"""
    try:
        data = request.get_json()
        
        # Check if position exists
        existing = Portfolio.query.filter_by(
            user_id=data['user_id'],
            symbol=data['symbol'].upper()
        ).first()
        
        if existing:
            # Update existing position
            existing.quantity = data['quantity']
            existing.avg_price = data['avg_price']
            existing.current_price = data['current_price']
            existing.pnl = (data['current_price'] - data['avg_price']) * data['quantity']
            existing.last_updated = datetime.utcnow()
        else:
            # Create new position
            position = Portfolio(
                user_id=data['user_id'],
                symbol=data['symbol'].upper(),
                quantity=data['quantity'],
                avg_price=data['avg_price'],
                current_price=data['current_price'],
                pnl=(data['current_price'] - data['avg_price']) * data['quantity']
            )
            db.session.add(position)
        
        db.session.commit()
        
        return jsonify({
            'success': True,
            'message': 'Portfolio updated successfully'
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# KM Nuggets Endpoints
@trading_bp.route('/km-nuggets', methods=['GET'])
def get_km_nuggets():
    """Get knowledge management nuggets"""
    try:
        category = request.args.get('category')
        limit = int(request.args.get('limit', 10))
        
        query = KMNugget.query.filter_by(is_active=True)
        if category:
            query = query.filter_by(category=category)
        
        nuggets = query.order_by(KMNugget.relevance_score.desc()).limit(limit).all()
        
        return jsonify({
            'success': True,
            'data': [nugget.to_dict() for nugget in nuggets]
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@trading_bp.route('/km-nuggets', methods=['POST'])
def create_km_nugget():
    """Create a new KM nugget"""
    try:
        data = request.get_json()
        
        nugget = KMNugget(
            title=data['title'],
            content=data['content'],
            category=data['category'],
            relevance_score=data.get('relevance_score', 0.5),
            market_impact=data.get('market_impact', 'MEDIUM'),
            symbols_affected=json.dumps(data.get('symbols_affected', []))
        )
        
        db.session.add(nugget)
        db.session.commit()
        
        return jsonify({
            'success': True,
            'data': nugget.to_dict()
        }), 201
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# Analytics Endpoints
@trading_bp.route('/analytics/performance', methods=['GET'])
def get_performance_analytics():
    """Get trading performance analytics"""
    try:
        user_id = request.args.get('user_id', type=int)
        
        # Calculate portfolio performance
        portfolio = Portfolio.query.filter_by(user_id=user_id).all() if user_id else []
        
        total_value = sum(item.quantity * item.current_price for item in portfolio)
        total_pnl = sum(item.pnl for item in portfolio)
        total_invested = sum(item.quantity * item.avg_price for item in portfolio)
        
        performance = {
            'total_value': total_value,
            'total_pnl': total_pnl,
            'total_invested': total_invested,
            'return_percentage': (total_pnl / total_invested * 100) if total_invested > 0 else 0,
            'positions_count': len(portfolio)
        }
        
        return jsonify({
            'success': True,
            'data': performance
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@trading_bp.route('/analytics/quantum-metrics', methods=['GET'])
def get_quantum_metrics():
    """Get quantum analysis metrics"""
    try:
        # Simulated quantum metrics for demo
        metrics = {
            'quantum_coherence': random.uniform(0.7, 0.95),
            'entanglement_strength': random.uniform(0.6, 0.9),
            'superposition_stability': random.uniform(0.8, 0.98),
            'decoherence_rate': random.uniform(0.01, 0.05),
            'quantum_advantage': random.uniform(1.2, 2.5),
            'bio_quantum_sync': random.uniform(0.75, 0.92)
        }
        
        return jsonify({
            'success': True,
            'data': metrics
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# Simulation/Demo Data Generation
@trading_bp.route('/demo/generate-data', methods=['POST'])
def generate_demo_data():
    """Generate demo data for testing (clearly labeled as simulation)"""
    try:
        # Generate sample market data
        symbols = ['BTC', 'ETH', 'AAPL', 'GOOGL', 'TSLA', 'NVDA', 'AMZN', 'MSFT']
        
        for symbol in symbols:
            # Clear existing data for symbol
            MarketData.query.filter_by(symbol=symbol).delete()
            
            # Generate new market data
            base_price = random.uniform(100, 50000)
            market_data = MarketData(
                symbol=symbol,
                price=base_price,
                volume=random.uniform(1000000, 100000000),
                change_24h=random.uniform(-base_price*0.1, base_price*0.1),
                change_percent_24h=random.uniform(-10, 10),
                market_cap=base_price * random.uniform(1000000, 1000000000),
                quantum_volatility=random.uniform(0.1, 1.0),
                bio_sentiment=random.uniform(0.1, 1.0)
            )
            db.session.add(market_data)
            
            # Generate trading signals
            signal = TradingSignal(
                symbol=symbol,
                signal_type=random.choice(['BUY', 'SELL', 'HOLD']),
                confidence=random.uniform(0.6, 0.95),
                price=base_price,
                quantum_score=random.uniform(0.5, 1.0),
                bio_indicator=random.uniform(0.5, 1.0)
            )
            db.session.add(signal)
        
        # Generate KM nuggets
        sample_nuggets = [
            {
                'title': 'Quantum Market Correlation Analysis',
                'content': 'Recent quantum analysis shows strong correlation between bio-rhythmic patterns and market volatility.',
                'category': 'QUANTUM_ANALYSIS',
                'market_impact': 'HIGH',
                'symbols_affected': ['BTC', 'ETH']
            },
            {
                'title': 'Bio-Sentiment Indicator Update',
                'content': 'Updated bio-sentiment algorithms show improved prediction accuracy for tech stocks.',
                'category': 'BIO_SENTIMENT',
                'market_impact': 'MEDIUM',
                'symbols_affected': ['AAPL', 'GOOGL', 'TSLA']
            }
        ]
        
        for nugget_data in sample_nuggets:
            nugget = KMNugget(
                title=nugget_data['title'],
                content=nugget_data['content'],
                category=nugget_data['category'],
                relevance_score=random.uniform(0.7, 1.0),
                market_impact=nugget_data['market_impact'],
                symbols_affected=json.dumps(nugget_data['symbols_affected'])
            )
            db.session.add(nugget)
        
        db.session.commit()
        
        return jsonify({
            'success': True,
            'message': 'Demo data generated successfully (SIMULATION DATA)',
            'note': 'This is simulated data for demonstration purposes only'
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

