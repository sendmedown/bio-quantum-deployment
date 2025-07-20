from flask_sqlalchemy import SQLAlchemy
from datetime import datetime
import json

db = SQLAlchemy()

class TradingSignal(db.Model):
    __tablename__ = 'trading_signals'
    
    id = db.Column(db.Integer, primary_key=True)
    symbol = db.Column(db.String(20), nullable=False)
    signal_type = db.Column(db.String(10), nullable=False)  # BUY, SELL, HOLD
    confidence = db.Column(db.Float, nullable=False)
    price = db.Column(db.Float, nullable=False)
    quantum_score = db.Column(db.Float, nullable=False)
    bio_indicator = db.Column(db.Float, nullable=False)
    timestamp = db.Column(db.DateTime, default=datetime.utcnow)
    is_active = db.Column(db.Boolean, default=True)
    
    def to_dict(self):
        return {
            'id': self.id,
            'symbol': self.symbol,
            'signal_type': self.signal_type,
            'confidence': self.confidence,
            'price': self.price,
            'quantum_score': self.quantum_score,
            'bio_indicator': self.bio_indicator,
            'timestamp': self.timestamp.isoformat(),
            'is_active': self.is_active
        }

class Portfolio(db.Model):
    __tablename__ = 'portfolios'
    
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False)
    symbol = db.Column(db.String(20), nullable=False)
    quantity = db.Column(db.Float, nullable=False)
    avg_price = db.Column(db.Float, nullable=False)
    current_price = db.Column(db.Float, nullable=False)
    pnl = db.Column(db.Float, default=0.0)
    last_updated = db.Column(db.DateTime, default=datetime.utcnow)
    
    def to_dict(self):
        return {
            'id': self.id,
            'user_id': self.user_id,
            'symbol': self.symbol,
            'quantity': self.quantity,
            'avg_price': self.avg_price,
            'current_price': self.current_price,
            'pnl': self.pnl,
            'last_updated': self.last_updated.isoformat()
        }

class KMNugget(db.Model):
    __tablename__ = 'km_nuggets'
    
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(200), nullable=False)
    content = db.Column(db.Text, nullable=False)
    category = db.Column(db.String(50), nullable=False)
    relevance_score = db.Column(db.Float, nullable=False)
    market_impact = db.Column(db.String(20), nullable=False)  # HIGH, MEDIUM, LOW
    symbols_affected = db.Column(db.Text)  # JSON array of symbols
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    is_active = db.Column(db.Boolean, default=True)
    
    def to_dict(self):
        return {
            'id': self.id,
            'title': self.title,
            'content': self.content,
            'category': self.category,
            'relevance_score': self.relevance_score,
            'market_impact': self.market_impact,
            'symbols_affected': json.loads(self.symbols_affected) if self.symbols_affected else [],
            'created_at': self.created_at.isoformat(),
            'is_active': self.is_active
        }

class MarketData(db.Model):
    __tablename__ = 'market_data'
    
    id = db.Column(db.Integer, primary_key=True)
    symbol = db.Column(db.String(20), nullable=False)
    price = db.Column(db.Float, nullable=False)
    volume = db.Column(db.Float, nullable=False)
    change_24h = db.Column(db.Float, nullable=False)
    change_percent_24h = db.Column(db.Float, nullable=False)
    market_cap = db.Column(db.Float)
    quantum_volatility = db.Column(db.Float, nullable=False)
    bio_sentiment = db.Column(db.Float, nullable=False)
    timestamp = db.Column(db.DateTime, default=datetime.utcnow)
    
    def to_dict(self):
        return {
            'id': self.id,
            'symbol': self.symbol,
            'price': self.price,
            'volume': self.volume,
            'change_24h': self.change_24h,
            'change_percent_24h': self.change_percent_24h,
            'market_cap': self.market_cap,
            'quantum_volatility': self.quantum_volatility,
            'bio_sentiment': self.bio_sentiment,
            'timestamp': self.timestamp.isoformat()
        }

