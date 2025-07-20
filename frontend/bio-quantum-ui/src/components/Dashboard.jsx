import { useState, useEffect } from 'react'
import { useWebSocket } from '../hooks/useWebSocket'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { 
  TrendingUp, 
  TrendingDown, 
  Activity, 
  Zap, 
  Brain,
  Target,
  AlertTriangle,
  DollarSign
} from 'lucide-react'
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, AreaChart, Area } from 'recharts'

const Dashboard = () => {
  const { marketData, tradingSignals, quantumMetrics, alerts } = useWebSocket()
  const [selectedSymbol, setSelectedSymbol] = useState('BTC')
  const [priceHistory, setPriceHistory] = useState([])

  // Generate sample price history for charts
  useEffect(() => {
    const generatePriceHistory = () => {
      const history = []
      let basePrice = 45000
      for (let i = 0; i < 24; i++) {
        basePrice += (Math.random() - 0.5) * 2000
        history.push({
          time: `${i}:00`,
          price: Math.max(basePrice, 30000),
          volume: Math.random() * 1000000
        })
      }
      return history
    }
    setPriceHistory(generatePriceHistory())
  }, [selectedSymbol])

  const symbols = ['BTC', 'ETH', 'AAPL', 'GOOGL', 'TSLA', 'NVDA', 'AMZN', 'MSFT']
  const currentData = marketData[selectedSymbol] || {}

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex flex-col md:flex-row md:items-center md:justify-between">
        <div>
          <h1 className="text-3xl font-bold text-white mb-2">Trading Dashboard</h1>
          <p className="text-gray-400">Real-time Bio-Quantum AI trading insights</p>
        </div>
        <div className="mt-4 md:mt-0">
          <Button 
            onClick={() => window.location.reload()} 
            className="bg-purple-600 hover:bg-purple-700"
          >
            <Activity className="w-4 h-4 mr-2" />
            Refresh Data
          </Button>
        </div>
      </div>

      {/* Quantum Metrics Overview */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        <Card className="bg-slate-800/50 border-purple-500/20">
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-gray-400 flex items-center">
              <Zap className="w-4 h-4 mr-2 text-yellow-500" />
              Quantum Coherence
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold text-white">
              {(quantumMetrics.quantum_coherence * 100)?.toFixed(1) || '0.0'}%
            </div>
            <p className="text-xs text-gray-500 mt-1">Quantum state stability</p>
          </CardContent>
        </Card>

        <Card className="bg-slate-800/50 border-purple-500/20">
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-gray-400 flex items-center">
              <Brain className="w-4 h-4 mr-2 text-purple-500" />
              Bio-Quantum Sync
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold text-white">
              {(quantumMetrics.bio_quantum_sync * 100)?.toFixed(1) || '0.0'}%
            </div>
            <p className="text-xs text-gray-500 mt-1">Bio-pattern alignment</p>
          </CardContent>
        </Card>

        <Card className="bg-slate-800/50 border-purple-500/20">
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-gray-400 flex items-center">
              <Target className="w-4 h-4 mr-2 text-green-500" />
              Quantum Advantage
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold text-white">
              {quantumMetrics.quantum_advantage?.toFixed(2) || '0.00'}x
            </div>
            <p className="text-xs text-gray-500 mt-1">Performance multiplier</p>
          </CardContent>
        </Card>

        <Card className="bg-slate-800/50 border-purple-500/20">
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-gray-400 flex items-center">
              <Activity className="w-4 h-4 mr-2 text-cyan-500" />
              Active Signals
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold text-white">
              {tradingSignals.length}
            </div>
            <p className="text-xs text-gray-500 mt-1">Real-time signals</p>
          </CardContent>
        </Card>
      </div>

      {/* Market Data and Charts */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Price Chart */}
        <Card className="lg:col-span-2 bg-slate-800/50 border-purple-500/20">
          <CardHeader>
            <div className="flex items-center justify-between">
              <CardTitle className="text-white">Price Chart</CardTitle>
              <div className="flex space-x-2">
                {symbols.slice(0, 4).map(symbol => (
                  <Button
                    key={symbol}
                    variant={selectedSymbol === symbol ? "default" : "outline"}
                    size="sm"
                    onClick={() => setSelectedSymbol(symbol)}
                    className={selectedSymbol === symbol ? "bg-purple-600" : ""}
                  >
                    {symbol}
                  </Button>
                ))}
              </div>
            </div>
          </CardHeader>
          <CardContent>
            <div className="h-64">
              <ResponsiveContainer width="100%" height="100%">
                <AreaChart data={priceHistory}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                  <XAxis dataKey="time" stroke="#9CA3AF" />
                  <YAxis stroke="#9CA3AF" />
                  <Tooltip 
                    contentStyle={{ 
                      backgroundColor: '#1F2937', 
                      border: '1px solid #6B7280',
                      borderRadius: '8px'
                    }}
                  />
                  <Area 
                    type="monotone" 
                    dataKey="price" 
                    stroke="#8B5CF6" 
                    fill="url(#colorPrice)"
                    strokeWidth={2}
                  />
                  <defs>
                    <linearGradient id="colorPrice" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%" stopColor="#8B5CF6" stopOpacity={0.3}/>
                      <stop offset="95%" stopColor="#8B5CF6" stopOpacity={0}/>
                    </linearGradient>
                  </defs>
                </AreaChart>
              </ResponsiveContainer>
            </div>
          </CardContent>
        </Card>

        {/* Market Overview */}
        <Card className="bg-slate-800/50 border-purple-500/20">
          <CardHeader>
            <CardTitle className="text-white">Market Overview</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            {symbols.slice(0, 6).map(symbol => {
              const data = marketData[symbol] || {}
              const isPositive = (data.change_percent_24h || 0) >= 0
              
              return (
                <div key={symbol} className="flex items-center justify-between">
                  <div className="flex items-center space-x-2">
                    <span className="font-medium text-white">{symbol}</span>
                    <Badge variant={isPositive ? "default" : "destructive"} className="text-xs">
                      {isPositive ? <TrendingUp className="w-3 h-3" /> : <TrendingDown className="w-3 h-3" />}
                    </Badge>
                  </div>
                  <div className="text-right">
                    <div className="text-white font-medium">
                      ${data.price?.toFixed(2) || '0.00'}
                    </div>
                    <div className={`text-xs ${isPositive ? 'text-green-400' : 'text-red-400'}`}>
                      {isPositive ? '+' : ''}{data.change_percent_24h?.toFixed(2) || '0.00'}%
                    </div>
                  </div>
                </div>
              )
            })}
          </CardContent>
        </Card>
      </div>

      {/* Recent Signals and Alerts */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Recent Trading Signals */}
        <Card className="bg-slate-800/50 border-purple-500/20">
          <CardHeader>
            <CardTitle className="text-white flex items-center">
              <TrendingUp className="w-5 h-5 mr-2 text-green-500" />
              Recent Signals
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-3">
              {tradingSignals.slice(0, 5).map((signal, index) => (
                <div key={index} className="flex items-center justify-between p-3 bg-slate-700/50 rounded-lg">
                  <div className="flex items-center space-x-3">
                    <Badge 
                      variant={signal.signal_type === 'BUY' ? 'default' : signal.signal_type === 'SELL' ? 'destructive' : 'secondary'}
                      className="text-xs"
                    >
                      {signal.signal_type}
                    </Badge>
                    <span className="text-white font-medium">{signal.symbol}</span>
                  </div>
                  <div className="text-right">
                    <div className="text-white">${signal.price?.toFixed(2)}</div>
                    <div className="text-xs text-gray-400">{signal.confidence?.toFixed(0)}% confidence</div>
                  </div>
                </div>
              ))}
              {tradingSignals.length === 0 && (
                <div className="text-center text-gray-400 py-4">
                  No recent signals available
                </div>
              )}
            </div>
          </CardContent>
        </Card>

        {/* Platform Alerts */}
        <Card className="bg-slate-800/50 border-purple-500/20">
          <CardHeader>
            <CardTitle className="text-white flex items-center">
              <AlertTriangle className="w-5 h-5 mr-2 text-yellow-500" />
              Platform Alerts
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-3">
              {alerts.slice(0, 5).map((alert, index) => (
                <div key={index} className="p-3 bg-slate-700/50 rounded-lg">
                  <div className="flex items-center justify-between mb-1">
                    <Badge variant="outline" className="text-xs">
                      {alert.type}
                    </Badge>
                    <span className="text-xs text-gray-400">
                      {new Date(alert.timestamp).toLocaleTimeString()}
                    </span>
                  </div>
                  <p className="text-sm text-gray-300">{alert.message}</p>
                </div>
              ))}
              {alerts.length === 0 && (
                <div className="text-center text-gray-400 py-4">
                  No recent alerts
                </div>
              )}
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  )
}

export default Dashboard

