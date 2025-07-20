import { useState, useEffect } from 'react'
import { useWebSocket } from '../hooks/useWebSocket'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { 
  BarChart3, 
  TrendingUp, 
  Activity, 
  Zap, 
  Brain,
  Target,
  Gauge,
  PieChart,
  LineChart,
  RefreshCw
} from 'lucide-react'
import { 
  LineChart as RechartsLineChart, 
  Line, 
  XAxis, 
  YAxis, 
  CartesianGrid, 
  Tooltip, 
  ResponsiveContainer,
  AreaChart,
  Area,
  BarChart,
  Bar,
  RadialBarChart,
  RadialBar,
  PieChart as RechartsPieChart,
  Cell,
  Pie
} from 'recharts'

const Analytics = () => {
  const { quantumMetrics } = useWebSocket()
  const [performance, setPerformance] = useState({})
  const [quantumHistory, setQuantumHistory] = useState([])
  const [performanceHistory, setPerformanceHistory] = useState([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    fetchAnalytics()
    generateHistoricalData()
  }, [])

  const fetchAnalytics = async () => {
    try {
      const [performanceRes, quantumRes] = await Promise.all([
        fetch('/api/trading/analytics/performance'),
        fetch('/api/trading/analytics/quantum-metrics')
      ])

      if (performanceRes.ok) {
        const perfData = await performanceRes.json()
        setPerformance(perfData.data || {})
      }

      setLoading(false)
    } catch (error) {
      console.error('Error fetching analytics:', error)
      setLoading(false)
    }
  }

  const generateHistoricalData = () => {
    // Generate sample historical data for charts
    const quantumData = []
    const performanceData = []
    
    for (let i = 0; i < 30; i++) {
      const date = new Date()
      date.setDate(date.getDate() - (29 - i))
      
      quantumData.push({
        date: date.toISOString().split('T')[0],
        coherence: Math.random() * 0.3 + 0.7,
        entanglement: Math.random() * 0.3 + 0.6,
        stability: Math.random() * 0.2 + 0.8,
        advantage: Math.random() * 1.3 + 1.2
      })
      
      performanceData.push({
        date: date.toISOString().split('T')[0],
        portfolio_value: 40000 + Math.random() * 10000,
        pnl: (Math.random() - 0.5) * 2000,
        return_pct: (Math.random() - 0.5) * 10
      })
    }
    
    setQuantumHistory(quantumData)
    setPerformanceHistory(performanceData)
  }

  // Quantum metrics for radial chart
  const quantumRadialData = [
    {
      name: 'Coherence',
      value: (quantumMetrics.quantum_coherence || 0.85) * 100,
      fill: '#8B5CF6'
    },
    {
      name: 'Entanglement',
      value: (quantumMetrics.entanglement_strength || 0.75) * 100,
      fill: '#06B6D4'
    },
    {
      name: 'Stability',
      value: (quantumMetrics.superposition_stability || 0.90) * 100,
      fill: '#10B981'
    },
    {
      name: 'Bio-Sync',
      value: (quantumMetrics.bio_quantum_sync || 0.82) * 100,
      fill: '#F59E0B'
    }
  ]

  // Performance distribution data
  const performanceDistribution = [
    { name: 'Profitable Trades', value: 65, fill: '#10B981' },
    { name: 'Break-even', value: 20, fill: '#F59E0B' },
    { name: 'Loss Trades', value: 15, fill: '#EF4444' }
  ]

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="text-white">Loading analytics...</div>
      </div>
    )
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex flex-col md:flex-row md:items-center md:justify-between">
        <div>
          <h1 className="text-3xl font-bold text-white mb-2">Analytics Dashboard</h1>
          <p className="text-gray-400">Advanced quantum metrics and performance analysis</p>
        </div>
        <div className="mt-4 md:mt-0">
          <Button 
            onClick={() => {
              fetchAnalytics()
              generateHistoricalData()
            }}
            className="bg-purple-600 hover:bg-purple-700"
          >
            <RefreshCw className="w-4 h-4 mr-2" />
            Refresh Analytics
          </Button>
        </div>
      </div>

      {/* Key Metrics */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        <Card className="bg-slate-800/50 border-purple-500/20">
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-gray-400 flex items-center">
              <BarChart3 className="w-4 h-4 mr-2 text-green-500" />
              Portfolio Value
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold text-white">
              ${(performance.total_value || 45550).toLocaleString()}
            </div>
            <p className="text-xs text-gray-500 mt-1">Current total value</p>
          </CardContent>
        </Card>

        <Card className="bg-slate-800/50 border-purple-500/20">
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-gray-400 flex items-center">
              <TrendingUp className="w-4 h-4 mr-2 text-purple-500" />
              Total Return
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold text-green-400">
              +{(performance.return_percentage || 7.18).toFixed(2)}%
            </div>
            <p className="text-xs text-gray-500 mt-1">Overall performance</p>
          </CardContent>
        </Card>

        <Card className="bg-slate-800/50 border-purple-500/20">
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-gray-400 flex items-center">
              <Zap className="w-4 h-4 mr-2 text-yellow-500" />
              Quantum Advantage
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold text-white">
              {(quantumMetrics.quantum_advantage || 1.85).toFixed(2)}x
            </div>
            <p className="text-xs text-gray-500 mt-1">Performance multiplier</p>
          </CardContent>
        </Card>

        <Card className="bg-slate-800/50 border-purple-500/20">
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-gray-400 flex items-center">
              <Target className="w-4 h-4 mr-2 text-cyan-500" />
              Active Positions
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold text-white">
              {performance.positions_count || 4}
            </div>
            <p className="text-xs text-gray-500 mt-1">Current holdings</p>
          </CardContent>
        </Card>
      </div>

      {/* Charts Row 1 */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Portfolio Performance Chart */}
        <Card className="bg-slate-800/50 border-purple-500/20">
          <CardHeader>
            <CardTitle className="text-white flex items-center">
              <LineChart className="w-5 h-5 mr-2" />
              Portfolio Performance (30 Days)
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="h-64">
              <ResponsiveContainer width="100%" height="100%">
                <AreaChart data={performanceHistory}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                  <XAxis dataKey="date" stroke="#9CA3AF" />
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
                    dataKey="portfolio_value" 
                    stroke="#8B5CF6" 
                    fill="url(#colorPortfolio)"
                    strokeWidth={2}
                  />
                  <defs>
                    <linearGradient id="colorPortfolio" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%" stopColor="#8B5CF6" stopOpacity={0.3}/>
                      <stop offset="95%" stopColor="#8B5CF6" stopOpacity={0}/>
                    </linearGradient>
                  </defs>
                </AreaChart>
              </ResponsiveContainer>
            </div>
          </CardContent>
        </Card>

        {/* Quantum Metrics Radial */}
        <Card className="bg-slate-800/50 border-purple-500/20">
          <CardHeader>
            <CardTitle className="text-white flex items-center">
              <Gauge className="w-5 h-5 mr-2" />
              Quantum Metrics
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="h-64">
              <ResponsiveContainer width="100%" height="100%">
                <RadialBarChart cx="50%" cy="50%" innerRadius="20%" outerRadius="80%" data={quantumRadialData}>
                  <RadialBar dataKey="value" cornerRadius={10} fill="#8884d8" />
                  <Tooltip 
                    formatter={(value) => [`${value.toFixed(1)}%`, 'Score']}
                    contentStyle={{ 
                      backgroundColor: '#1F2937', 
                      border: '1px solid #6B7280',
                      borderRadius: '8px'
                    }}
                  />
                </RadialBarChart>
              </ResponsiveContainer>
            </div>
            <div className="grid grid-cols-2 gap-2 mt-4">
              {quantumRadialData.map((item, index) => (
                <div key={index} className="flex items-center space-x-2">
                  <div 
                    className="w-3 h-3 rounded-full" 
                    style={{ backgroundColor: item.fill }}
                  />
                  <span className="text-sm text-gray-300">{item.name}</span>
                  <span className="text-sm text-white font-medium">{item.value.toFixed(1)}%</span>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Charts Row 2 */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Quantum Coherence History */}
        <Card className="bg-slate-800/50 border-purple-500/20">
          <CardHeader>
            <CardTitle className="text-white flex items-center">
              <Brain className="w-5 h-5 mr-2" />
              Quantum Coherence Trends
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="h-64">
              <ResponsiveContainer width="100%" height="100%">
                <RechartsLineChart data={quantumHistory}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                  <XAxis dataKey="date" stroke="#9CA3AF" />
                  <YAxis stroke="#9CA3AF" />
                  <Tooltip 
                    contentStyle={{ 
                      backgroundColor: '#1F2937', 
                      border: '1px solid #6B7280',
                      borderRadius: '8px'
                    }}
                  />
                  <Line 
                    type="monotone" 
                    dataKey="coherence" 
                    stroke="#8B5CF6" 
                    strokeWidth={2}
                    name="Coherence"
                  />
                  <Line 
                    type="monotone" 
                    dataKey="entanglement" 
                    stroke="#06B6D4" 
                    strokeWidth={2}
                    name="Entanglement"
                  />
                  <Line 
                    type="monotone" 
                    dataKey="stability" 
                    stroke="#10B981" 
                    strokeWidth={2}
                    name="Stability"
                  />
                </RechartsLineChart>
              </ResponsiveContainer>
            </div>
          </CardContent>
        </Card>

        {/* Trade Performance Distribution */}
        <Card className="bg-slate-800/50 border-purple-500/20">
          <CardHeader>
            <CardTitle className="text-white flex items-center">
              <PieChart className="w-5 h-5 mr-2" />
              Trade Performance Distribution
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="h-64">
              <ResponsiveContainer width="100%" height="100%">
                <RechartsPieChart>
                  <Pie
                    data={performanceDistribution}
                    cx="50%"
                    cy="50%"
                    innerRadius={40}
                    outerRadius={80}
                    paddingAngle={5}
                    dataKey="value"
                  >
                    {performanceDistribution.map((entry, index) => (
                      <Cell key={`cell-${index}`} fill={entry.fill} />
                    ))}
                  </Pie>
                  <Tooltip 
                    formatter={(value) => [`${value}%`, 'Percentage']}
                    contentStyle={{ 
                      backgroundColor: '#1F2937', 
                      border: '1px solid #6B7280',
                      borderRadius: '8px'
                    }}
                  />
                </RechartsPieChart>
              </ResponsiveContainer>
            </div>
            <div className="mt-4 space-y-2">
              {performanceDistribution.map((item, index) => (
                <div key={index} className="flex items-center justify-between">
                  <div className="flex items-center space-x-2">
                    <div 
                      className="w-3 h-3 rounded-full" 
                      style={{ backgroundColor: item.fill }}
                    />
                    <span className="text-sm text-gray-300">{item.name}</span>
                  </div>
                  <span className="text-sm text-white font-medium">{item.value}%</span>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Detailed Metrics */}
      <Card className="bg-slate-800/50 border-purple-500/20">
        <CardHeader>
          <CardTitle className="text-white flex items-center">
            <Activity className="w-5 h-5 mr-2" />
            Detailed Quantum Analysis
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
            <div className="space-y-3">
              <h4 className="text-lg font-semibold text-white">Quantum States</h4>
              <div className="space-y-2">
                <div className="flex justify-between">
                  <span className="text-gray-400">Coherence Level</span>
                  <span className="text-purple-400">{((quantumMetrics.quantum_coherence || 0.85) * 100).toFixed(1)}%</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-gray-400">Entanglement Strength</span>
                  <span className="text-cyan-400">{((quantumMetrics.entanglement_strength || 0.75) * 100).toFixed(1)}%</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-gray-400">Superposition Stability</span>
                  <span className="text-green-400">{((quantumMetrics.superposition_stability || 0.90) * 100).toFixed(1)}%</span>
                </div>
              </div>
            </div>
            
            <div className="space-y-3">
              <h4 className="text-lg font-semibold text-white">Bio-Quantum Sync</h4>
              <div className="space-y-2">
                <div className="flex justify-between">
                  <span className="text-gray-400">Sync Rate</span>
                  <span className="text-yellow-400">{((quantumMetrics.bio_quantum_sync || 0.82) * 100).toFixed(1)}%</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-gray-400">Decoherence Rate</span>
                  <span className="text-red-400">{((quantumMetrics.decoherence_rate || 0.03) * 100).toFixed(2)}%</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-gray-400">Quantum Advantage</span>
                  <span className="text-white">{(quantumMetrics.quantum_advantage || 1.85).toFixed(2)}x</span>
                </div>
              </div>
            </div>
            
            <div className="space-y-3">
              <h4 className="text-lg font-semibold text-white">Performance Metrics</h4>
              <div className="space-y-2">
                <div className="flex justify-between">
                  <span className="text-gray-400">Win Rate</span>
                  <span className="text-green-400">65%</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-gray-400">Sharpe Ratio</span>
                  <span className="text-white">2.34</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-gray-400">Max Drawdown</span>
                  <span className="text-red-400">-8.5%</span>
                </div>
              </div>
            </div>
          </div>
        </CardContent>
      </Card>
    </div>
  )
}

export default Analytics

