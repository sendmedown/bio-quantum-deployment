import { useState, useEffect } from 'react'
import { useWebSocket } from '../hooks/useWebSocket'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Input } from '@/components/ui/input'
import { 
  TrendingUp, 
  TrendingDown, 
  Target, 
  Zap, 
  Brain,
  Filter,
  Search,
  Clock,
  AlertCircle
} from 'lucide-react'

const Signals = () => {
  const { tradingSignals } = useWebSocket()
  const [allSignals, setAllSignals] = useState([])
  const [filteredSignals, setFilteredSignals] = useState([])
  const [searchTerm, setSearchTerm] = useState('')
  const [selectedType, setSelectedType] = useState('ALL')
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    fetchSignals()
  }, [])

  useEffect(() => {
    // Combine WebSocket signals with fetched signals
    const combined = [...tradingSignals, ...allSignals]
    const unique = combined.filter((signal, index, self) => 
      index === self.findIndex(s => s.id === signal.id || (s.symbol === signal.symbol && s.timestamp === signal.timestamp))
    )
    
    // Apply filters
    let filtered = unique
    
    if (searchTerm) {
      filtered = filtered.filter(signal => 
        signal.symbol.toLowerCase().includes(searchTerm.toLowerCase())
      )
    }
    
    if (selectedType !== 'ALL') {
      filtered = filtered.filter(signal => signal.signal_type === selectedType)
    }
    
    // Sort by timestamp (newest first)
    filtered.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp))
    
    setFilteredSignals(filtered)
  }, [tradingSignals, allSignals, searchTerm, selectedType])

  const fetchSignals = async () => {
    try {
      const response = await fetch('/api/trading/signals')
      if (response.ok) {
        const data = await response.json()
        setAllSignals(data.data || [])
      }
      setLoading(false)
    } catch (error) {
      console.error('Error fetching signals:', error)
      setLoading(false)
    }
  }

  const generateDemoData = async () => {
    try {
      const response = await fetch('/api/trading/demo/generate-data', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        }
      })
      
      if (response.ok) {
        await fetchSignals()
      }
    } catch (error) {
      console.error('Error generating demo data:', error)
    }
  }

  const getSignalColor = (signalType) => {
    switch (signalType) {
      case 'BUY': return 'bg-green-500/20 text-green-400 border-green-500/30'
      case 'SELL': return 'bg-red-500/20 text-red-400 border-red-500/30'
      case 'HOLD': return 'bg-yellow-500/20 text-yellow-400 border-yellow-500/30'
      default: return 'bg-gray-500/20 text-gray-400 border-gray-500/30'
    }
  }

  const getSignalIcon = (signalType) => {
    switch (signalType) {
      case 'BUY': return <TrendingUp className="w-4 h-4" />
      case 'SELL': return <TrendingDown className="w-4 h-4" />
      case 'HOLD': return <Target className="w-4 h-4" />
      default: return <AlertCircle className="w-4 h-4" />
    }
  }

  const getConfidenceColor = (confidence) => {
    if (confidence >= 0.8) return 'text-green-400'
    if (confidence >= 0.6) return 'text-yellow-400'
    return 'text-red-400'
  }

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="text-white">Loading signals...</div>
      </div>
    )
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex flex-col md:flex-row md:items-center md:justify-between">
        <div>
          <h1 className="text-3xl font-bold text-white mb-2">Trading Signals</h1>
          <p className="text-gray-400">AI-powered trading recommendations with quantum analysis</p>
        </div>
        <div className="mt-4 md:mt-0 space-x-2">
          <Button 
            onClick={generateDemoData}
            variant="outline"
            className="border-purple-500 text-purple-400 hover:bg-purple-500/10"
          >
            <Zap className="w-4 h-4 mr-2" />
            Generate Demo Signals
          </Button>
          <Button 
            onClick={fetchSignals}
            className="bg-purple-600 hover:bg-purple-700"
          >
            <TrendingUp className="w-4 h-4 mr-2" />
            Refresh
          </Button>
        </div>
      </div>

      {/* Filters */}
      <Card className="bg-slate-800/50 border-purple-500/20">
        <CardHeader>
          <CardTitle className="text-white flex items-center">
            <Filter className="w-5 h-5 mr-2" />
            Filters
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex flex-col md:flex-row gap-4">
            {/* Search */}
            <div className="flex-1">
              <div className="relative">
                <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 w-4 h-4" />
                <Input
                  placeholder="Search by symbol..."
                  value={searchTerm}
                  onChange={(e) => setSearchTerm(e.target.value)}
                  className="pl-10 bg-slate-700 border-slate-600 text-white"
                />
              </div>
            </div>
            
            {/* Signal Type Filter */}
            <div className="flex space-x-2">
              {['ALL', 'BUY', 'SELL', 'HOLD'].map(type => (
                <Button
                  key={type}
                  variant={selectedType === type ? "default" : "outline"}
                  size="sm"
                  onClick={() => setSelectedType(type)}
                  className={selectedType === type ? "bg-purple-600" : "border-slate-600 text-gray-300"}
                >
                  {type}
                </Button>
              ))}
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Signals Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
        {filteredSignals.map((signal, index) => (
          <Card key={signal.id || index} className="bg-slate-800/50 border-purple-500/20 hover:border-purple-400/40 transition-colors">
            <CardHeader className="pb-3">
              <div className="flex items-center justify-between">
                <div className="flex items-center space-x-2">
                  <div className="w-8 h-8 bg-gradient-to-r from-purple-500 to-cyan-500 rounded-lg flex items-center justify-center text-white font-bold text-sm">
                    {signal.symbol.slice(0, 2)}
                  </div>
                  <div>
                    <h3 className="font-semibold text-white">{signal.symbol}</h3>
                    <p className="text-xs text-gray-400">
                      {new Date(signal.timestamp).toLocaleString()}
                    </p>
                  </div>
                </div>
                <Badge className={getSignalColor(signal.signal_type)}>
                  {getSignalIcon(signal.signal_type)}
                  <span className="ml-1">{signal.signal_type}</span>
                </Badge>
              </div>
            </CardHeader>
            
            <CardContent className="space-y-3">
              {/* Price */}
              <div className="flex items-center justify-between">
                <span className="text-gray-400">Price</span>
                <span className="text-white font-medium">${signal.price?.toFixed(2) || '0.00'}</span>
              </div>
              
              {/* Confidence */}
              <div className="flex items-center justify-between">
                <span className="text-gray-400">Confidence</span>
                <span className={`font-medium ${getConfidenceColor(signal.confidence)}`}>
                  {((signal.confidence || 0) * 100).toFixed(0)}%
                </span>
              </div>
              
              {/* Quantum Score */}
              <div className="flex items-center justify-between">
                <span className="text-gray-400 flex items-center">
                  <Zap className="w-3 h-3 mr-1" />
                  Quantum Score
                </span>
                <span className="text-purple-400 font-medium">
                  {(signal.quantum_score || 0).toFixed(3)}
                </span>
              </div>
              
              {/* Bio Indicator */}
              <div className="flex items-center justify-between">
                <span className="text-gray-400 flex items-center">
                  <Brain className="w-3 h-3 mr-1" />
                  Bio Indicator
                </span>
                <span className="text-cyan-400 font-medium">
                  {(signal.bio_indicator || 0).toFixed(3)}
                </span>
              </div>
              
              {/* Note for simulated signals */}
              {signal.note && (
                <div className="mt-3 p-2 bg-yellow-500/10 border border-yellow-500/20 rounded text-xs text-yellow-400">
                  {signal.note}
                </div>
              )}
              
              {/* Action Button */}
              <Button 
                className={`w-full mt-3 ${
                  signal.signal_type === 'BUY' 
                    ? 'bg-green-600 hover:bg-green-700' 
                    : signal.signal_type === 'SELL'
                    ? 'bg-red-600 hover:bg-red-700'
                    : 'bg-yellow-600 hover:bg-yellow-700'
                }`}
                size="sm"
              >
                {signal.signal_type === 'BUY' && 'Execute Buy Order'}
                {signal.signal_type === 'SELL' && 'Execute Sell Order'}
                {signal.signal_type === 'HOLD' && 'Monitor Position'}
              </Button>
            </CardContent>
          </Card>
        ))}
      </div>
      
      {filteredSignals.length === 0 && (
        <Card className="bg-slate-800/50 border-purple-500/20">
          <CardContent className="text-center py-12">
            <TrendingUp className="w-12 h-12 mx-auto mb-4 text-gray-500" />
            <h3 className="text-lg font-medium text-white mb-2">No signals found</h3>
            <p className="text-gray-400 mb-4">
              {searchTerm || selectedType !== 'ALL' 
                ? 'Try adjusting your filters or search terms'
                : 'Generate demo data to see sample trading signals'
              }
            </p>
            {!searchTerm && selectedType === 'ALL' && (
              <Button 
                onClick={generateDemoData}
                className="bg-purple-600 hover:bg-purple-700"
              >
                <Zap className="w-4 h-4 mr-2" />
                Generate Demo Signals
              </Button>
            )}
          </CardContent>
        </Card>
      )}
    </div>
  )
}

export default Signals

