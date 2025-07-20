import { useState, useEffect } from 'react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { 
  TrendingUp, 
  TrendingDown, 
  DollarSign, 
  Percent,
  PieChart,
  BarChart3,
  Plus,
  Minus
} from 'lucide-react'
import { PieChart as RechartsPieChart, Cell, Pie, ResponsiveContainer, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip } from 'recharts'

const Portfolio = () => {
  const [portfolio, setPortfolio] = useState([])
  const [performance, setPerformance] = useState({})
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    fetchPortfolioData()
    fetchPerformanceData()
  }, [])

  const fetchPortfolioData = async () => {
    try {
      // Simulated portfolio data - in real app, this would come from API
      const mockPortfolio = [
        {
          id: 1,
          symbol: 'BTC',
          quantity: 0.5,
          avg_price: 42000,
          current_price: 45000,
          pnl: 1500,
          last_updated: new Date().toISOString()
        },
        {
          id: 2,
          symbol: 'ETH',
          quantity: 2.5,
          avg_price: 2800,
          current_price: 3200,
          pnl: 1000,
          last_updated: new Date().toISOString()
        },
        {
          id: 3,
          symbol: 'AAPL',
          quantity: 10,
          avg_price: 180,
          current_price: 185,
          pnl: 50,
          last_updated: new Date().toISOString()
        },
        {
          id: 4,
          symbol: 'GOOGL',
          quantity: 5,
          avg_price: 2500,
          current_price: 2600,
          pnl: 500,
          last_updated: new Date().toISOString()
        }
      ]
      setPortfolio(mockPortfolio)
      setLoading(false)
    } catch (error) {
      console.error('Error fetching portfolio:', error)
      setLoading(false)
    }
  }

  const fetchPerformanceData = async () => {
    try {
      // Simulated performance data
      const mockPerformance = {
        total_value: 45550,
        total_pnl: 3050,
        total_invested: 42500,
        return_percentage: 7.18,
        positions_count: 4
      }
      setPerformance(mockPerformance)
    } catch (error) {
      console.error('Error fetching performance:', error)
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
        await fetchPortfolioData()
        await fetchPerformanceData()
      }
    } catch (error) {
      console.error('Error generating demo data:', error)
    }
  }

  // Calculate portfolio distribution for pie chart
  const portfolioDistribution = portfolio.map(item => ({
    name: item.symbol,
    value: item.quantity * item.current_price,
    color: getColorForSymbol(item.symbol)
  }))

  function getColorForSymbol(symbol) {
    const colors = {
      'BTC': '#F7931A',
      'ETH': '#627EEA',
      'AAPL': '#007AFF',
      'GOOGL': '#4285F4',
      'TSLA': '#CC0000',
      'NVDA': '#76B900'
    }
    return colors[symbol] || '#8B5CF6'
  }

  const totalValue = portfolio.reduce((sum, item) => sum + (item.quantity * item.current_price), 0)
  const totalPnL = portfolio.reduce((sum, item) => sum + item.pnl, 0)

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="text-white">Loading portfolio...</div>
      </div>
    )
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex flex-col md:flex-row md:items-center md:justify-between">
        <div>
          <h1 className="text-3xl font-bold text-white mb-2">Portfolio</h1>
          <p className="text-gray-400">Track your trading positions and performance</p>
        </div>
        <div className="mt-4 md:mt-0 space-x-2">
          <Button 
            onClick={generateDemoData}
            variant="outline"
            className="border-purple-500 text-purple-400 hover:bg-purple-500/10"
          >
            <Plus className="w-4 h-4 mr-2" />
            Generate Demo Data
          </Button>
          <Button 
            onClick={fetchPortfolioData}
            className="bg-purple-600 hover:bg-purple-700"
          >
            <BarChart3 className="w-4 h-4 mr-2" />
            Refresh
          </Button>
        </div>
      </div>

      {/* Performance Overview */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        <Card className="bg-slate-800/50 border-purple-500/20">
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-gray-400 flex items-center">
              <DollarSign className="w-4 h-4 mr-2 text-green-500" />
              Total Value
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold text-white">
              ${totalValue.toFixed(2)}
            </div>
            <p className="text-xs text-gray-500 mt-1">Current portfolio value</p>
          </CardContent>
        </Card>

        <Card className="bg-slate-800/50 border-purple-500/20">
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-gray-400 flex items-center">
              <TrendingUp className="w-4 h-4 mr-2 text-purple-500" />
              Total P&L
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className={`text-2xl font-bold ${totalPnL >= 0 ? 'text-green-400' : 'text-red-400'}`}>
              {totalPnL >= 0 ? '+' : ''}${totalPnL.toFixed(2)}
            </div>
            <p className="text-xs text-gray-500 mt-1">Unrealized gains/losses</p>
          </CardContent>
        </Card>

        <Card className="bg-slate-800/50 border-purple-500/20">
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-gray-400 flex items-center">
              <Percent className="w-4 h-4 mr-2 text-cyan-500" />
              Return %
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className={`text-2xl font-bold ${totalPnL >= 0 ? 'text-green-400' : 'text-red-400'}`}>
              {totalPnL >= 0 ? '+' : ''}{((totalPnL / (totalValue - totalPnL)) * 100).toFixed(2)}%
            </div>
            <p className="text-xs text-gray-500 mt-1">Portfolio return</p>
          </CardContent>
        </Card>

        <Card className="bg-slate-800/50 border-purple-500/20">
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-gray-400 flex items-center">
              <PieChart className="w-4 h-4 mr-2 text-yellow-500" />
              Positions
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold text-white">
              {portfolio.length}
            </div>
            <p className="text-xs text-gray-500 mt-1">Active positions</p>
          </CardContent>
        </Card>
      </div>

      {/* Portfolio Distribution and Holdings */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Portfolio Distribution Chart */}
        <Card className="bg-slate-800/50 border-purple-500/20">
          <CardHeader>
            <CardTitle className="text-white">Portfolio Distribution</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="h-64">
              <ResponsiveContainer width="100%" height="100%">
                <RechartsPieChart>
                  <Pie
                    data={portfolioDistribution}
                    cx="50%"
                    cy="50%"
                    innerRadius={40}
                    outerRadius={80}
                    paddingAngle={5}
                    dataKey="value"
                  >
                    {portfolioDistribution.map((entry, index) => (
                      <Cell key={`cell-${index}`} fill={entry.color} />
                    ))}
                  </Pie>
                  <Tooltip 
                    formatter={(value) => [`$${value.toFixed(2)}`, 'Value']}
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
              {portfolioDistribution.map((item, index) => (
                <div key={index} className="flex items-center justify-between">
                  <div className="flex items-center space-x-2">
                    <div 
                      className="w-3 h-3 rounded-full" 
                      style={{ backgroundColor: item.color }}
                    />
                    <span className="text-sm text-gray-300">{item.name}</span>
                  </div>
                  <span className="text-sm text-white">
                    {((item.value / totalValue) * 100).toFixed(1)}%
                  </span>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>

        {/* Holdings List */}
        <Card className="lg:col-span-2 bg-slate-800/50 border-purple-500/20">
          <CardHeader>
            <CardTitle className="text-white">Holdings</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-4">
              {portfolio.map((position) => {
                const currentValue = position.quantity * position.current_price
                const investedValue = position.quantity * position.avg_price
                const returnPercent = ((position.pnl / investedValue) * 100)
                const isPositive = position.pnl >= 0

                return (
                  <div key={position.id} className="p-4 bg-slate-700/50 rounded-lg">
                    <div className="flex items-center justify-between mb-3">
                      <div className="flex items-center space-x-3">
                        <div 
                          className="w-10 h-10 rounded-full flex items-center justify-center text-white font-bold"
                          style={{ backgroundColor: getColorForSymbol(position.symbol) }}
                        >
                          {position.symbol.slice(0, 2)}
                        </div>
                        <div>
                          <h3 className="font-semibold text-white">{position.symbol}</h3>
                          <p className="text-sm text-gray-400">{position.quantity} shares</p>
                        </div>
                      </div>
                      <Badge 
                        variant={isPositive ? "default" : "destructive"}
                        className="flex items-center space-x-1"
                      >
                        {isPositive ? <TrendingUp className="w-3 h-3" /> : <TrendingDown className="w-3 h-3" />}
                        <span>{isPositive ? '+' : ''}{returnPercent.toFixed(2)}%</span>
                      </Badge>
                    </div>
                    
                    <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
                      <div>
                        <p className="text-gray-400">Current Price</p>
                        <p className="text-white font-medium">${position.current_price.toFixed(2)}</p>
                      </div>
                      <div>
                        <p className="text-gray-400">Avg Price</p>
                        <p className="text-white font-medium">${position.avg_price.toFixed(2)}</p>
                      </div>
                      <div>
                        <p className="text-gray-400">Market Value</p>
                        <p className="text-white font-medium">${currentValue.toFixed(2)}</p>
                      </div>
                      <div>
                        <p className="text-gray-400">P&L</p>
                        <p className={`font-medium ${isPositive ? 'text-green-400' : 'text-red-400'}`}>
                          {isPositive ? '+' : ''}${position.pnl.toFixed(2)}
                        </p>
                      </div>
                    </div>
                  </div>
                )
              })}
              
              {portfolio.length === 0 && (
                <div className="text-center text-gray-400 py-8">
                  <PieChart className="w-12 h-12 mx-auto mb-4 opacity-50" />
                  <p>No positions found</p>
                  <p className="text-sm mt-2">Generate demo data to see sample portfolio</p>
                </div>
              )}
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  )
}

export default Portfolio

