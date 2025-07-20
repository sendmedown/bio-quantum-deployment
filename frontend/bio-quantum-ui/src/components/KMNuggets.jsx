import { useState, useEffect } from 'react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Input } from '@/components/ui/input'
import { 
  Brain, 
  Search, 
  Filter,
  TrendingUp,
  AlertTriangle,
  Info,
  Lightbulb,
  Target,
  Clock,
  Tag
} from 'lucide-react'

const KMNuggets = () => {
  const [nuggets, setNuggets] = useState([])
  const [filteredNuggets, setFilteredNuggets] = useState([])
  const [searchTerm, setSearchTerm] = useState('')
  const [selectedCategory, setSelectedCategory] = useState('ALL')
  const [selectedImpact, setSelectedImpact] = useState('ALL')
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    fetchNuggets()
  }, [])

  useEffect(() => {
    let filtered = nuggets

    if (searchTerm) {
      filtered = filtered.filter(nugget => 
        nugget.title.toLowerCase().includes(searchTerm.toLowerCase()) ||
        nugget.content.toLowerCase().includes(searchTerm.toLowerCase())
      )
    }

    if (selectedCategory !== 'ALL') {
      filtered = filtered.filter(nugget => nugget.category === selectedCategory)
    }

    if (selectedImpact !== 'ALL') {
      filtered = filtered.filter(nugget => nugget.market_impact === selectedImpact)
    }

    // Sort by relevance score (highest first)
    filtered.sort((a, b) => b.relevance_score - a.relevance_score)

    setFilteredNuggets(filtered)
  }, [nuggets, searchTerm, selectedCategory, selectedImpact])

  const fetchNuggets = async () => {
    try {
      const response = await fetch('/api/trading/km-nuggets')
      if (response.ok) {
        const data = await response.json()
        setNuggets(data.data || [])
      }
      setLoading(false)
    } catch (error) {
      console.error('Error fetching KM nuggets:', error)
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
        await fetchNuggets()
      }
    } catch (error) {
      console.error('Error generating demo data:', error)
    }
  }

  const getCategoryIcon = (category) => {
    switch (category) {
      case 'QUANTUM_ANALYSIS': return <Brain className="w-4 h-4" />
      case 'BIO_SENTIMENT': return <Target className="w-4 h-4" />
      case 'MARKET_INSIGHT': return <TrendingUp className="w-4 h-4" />
      case 'RISK_ANALYSIS': return <AlertTriangle className="w-4 h-4" />
      default: return <Info className="w-4 h-4" />
    }
  }

  const getCategoryColor = (category) => {
    switch (category) {
      case 'QUANTUM_ANALYSIS': return 'bg-purple-500/20 text-purple-400 border-purple-500/30'
      case 'BIO_SENTIMENT': return 'bg-cyan-500/20 text-cyan-400 border-cyan-500/30'
      case 'MARKET_INSIGHT': return 'bg-green-500/20 text-green-400 border-green-500/30'
      case 'RISK_ANALYSIS': return 'bg-red-500/20 text-red-400 border-red-500/30'
      default: return 'bg-gray-500/20 text-gray-400 border-gray-500/30'
    }
  }

  const getImpactColor = (impact) => {
    switch (impact) {
      case 'HIGH': return 'bg-red-500/20 text-red-400 border-red-500/30'
      case 'MEDIUM': return 'bg-yellow-500/20 text-yellow-400 border-yellow-500/30'
      case 'LOW': return 'bg-green-500/20 text-green-400 border-green-500/30'
      default: return 'bg-gray-500/20 text-gray-400 border-gray-500/30'
    }
  }

  const getRelevanceColor = (score) => {
    if (score >= 0.8) return 'text-green-400'
    if (score >= 0.6) return 'text-yellow-400'
    return 'text-red-400'
  }

  const categories = ['ALL', 'QUANTUM_ANALYSIS', 'BIO_SENTIMENT', 'MARKET_INSIGHT', 'RISK_ANALYSIS']
  const impacts = ['ALL', 'HIGH', 'MEDIUM', 'LOW']

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="text-white">Loading knowledge nuggets...</div>
      </div>
    )
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex flex-col md:flex-row md:items-center md:justify-between">
        <div>
          <h1 className="text-3xl font-bold text-white mb-2">Knowledge Management</h1>
          <p className="text-gray-400">AI-curated insights and market intelligence nuggets</p>
        </div>
        <div className="mt-4 md:mt-0 space-x-2">
          <Button 
            onClick={generateDemoData}
            variant="outline"
            className="border-purple-500 text-purple-400 hover:bg-purple-500/10"
          >
            <Lightbulb className="w-4 h-4 mr-2" />
            Generate Demo Nuggets
          </Button>
          <Button 
            onClick={fetchNuggets}
            className="bg-purple-600 hover:bg-purple-700"
          >
            <Brain className="w-4 h-4 mr-2" />
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
          <div className="space-y-4">
            {/* Search */}
            <div className="relative">
              <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 w-4 h-4" />
              <Input
                placeholder="Search nuggets by title or content..."
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                className="pl-10 bg-slate-700 border-slate-600 text-white"
              />
            </div>
            
            {/* Category Filter */}
            <div>
              <label className="text-sm text-gray-400 mb-2 block">Category</label>
              <div className="flex flex-wrap gap-2">
                {categories.map(category => (
                  <Button
                    key={category}
                    variant={selectedCategory === category ? "default" : "outline"}
                    size="sm"
                    onClick={() => setSelectedCategory(category)}
                    className={selectedCategory === category ? "bg-purple-600" : "border-slate-600 text-gray-300"}
                  >
                    {category.replace('_', ' ')}
                  </Button>
                ))}
              </div>
            </div>
            
            {/* Impact Filter */}
            <div>
              <label className="text-sm text-gray-400 mb-2 block">Market Impact</label>
              <div className="flex gap-2">
                {impacts.map(impact => (
                  <Button
                    key={impact}
                    variant={selectedImpact === impact ? "default" : "outline"}
                    size="sm"
                    onClick={() => setSelectedImpact(impact)}
                    className={selectedImpact === impact ? "bg-purple-600" : "border-slate-600 text-gray-300"}
                  >
                    {impact}
                  </Button>
                ))}
              </div>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Nuggets Grid */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {filteredNuggets.map((nugget) => (
          <Card key={nugget.id} className="bg-slate-800/50 border-purple-500/20 hover:border-purple-400/40 transition-colors">
            <CardHeader>
              <div className="flex items-start justify-between">
                <div className="flex-1">
                  <div className="flex items-center space-x-2 mb-2">
                    <Badge className={getCategoryColor(nugget.category)}>
                      {getCategoryIcon(nugget.category)}
                      <span className="ml-1">{nugget.category.replace('_', ' ')}</span>
                    </Badge>
                    <Badge className={getImpactColor(nugget.market_impact)}>
                      {nugget.market_impact} IMPACT
                    </Badge>
                  </div>
                  <h3 className="text-lg font-semibold text-white mb-2">{nugget.title}</h3>
                  <div className="flex items-center space-x-4 text-sm text-gray-400">
                    <div className="flex items-center space-x-1">
                      <Target className="w-3 h-3" />
                      <span>Relevance: </span>
                      <span className={getRelevanceColor(nugget.relevance_score)}>
                        {(nugget.relevance_score * 100).toFixed(0)}%
                      </span>
                    </div>
                    <div className="flex items-center space-x-1">
                      <Clock className="w-3 h-3" />
                      <span>{new Date(nugget.created_at).toLocaleDateString()}</span>
                    </div>
                  </div>
                </div>
              </div>
            </CardHeader>
            
            <CardContent className="space-y-4">
              {/* Content */}
              <p className="text-gray-300 leading-relaxed">{nugget.content}</p>
              
              {/* Affected Symbols */}
              {nugget.symbols_affected && nugget.symbols_affected.length > 0 && (
                <div>
                  <div className="flex items-center space-x-2 mb-2">
                    <Tag className="w-4 h-4 text-gray-400" />
                    <span className="text-sm text-gray-400">Affected Symbols:</span>
                  </div>
                  <div className="flex flex-wrap gap-1">
                    {nugget.symbols_affected.map((symbol, index) => (
                      <Badge key={index} variant="outline" className="text-xs border-slate-600 text-gray-300">
                        {symbol}
                      </Badge>
                    ))}
                  </div>
                </div>
              )}
              
              {/* Actions */}
              <div className="flex space-x-2 pt-2">
                <Button size="sm" variant="outline" className="border-slate-600 text-gray-300 hover:bg-slate-700">
                  <Info className="w-3 h-3 mr-1" />
                  Details
                </Button>
                <Button size="sm" variant="outline" className="border-slate-600 text-gray-300 hover:bg-slate-700">
                  <TrendingUp className="w-3 h-3 mr-1" />
                  Apply Insight
                </Button>
              </div>
            </CardContent>
          </Card>
        ))}
      </div>
      
      {filteredNuggets.length === 0 && (
        <Card className="bg-slate-800/50 border-purple-500/20">
          <CardContent className="text-center py-12">
            <Brain className="w-12 h-12 mx-auto mb-4 text-gray-500" />
            <h3 className="text-lg font-medium text-white mb-2">No knowledge nuggets found</h3>
            <p className="text-gray-400 mb-4">
              {searchTerm || selectedCategory !== 'ALL' || selectedImpact !== 'ALL'
                ? 'Try adjusting your filters or search terms'
                : 'Generate demo data to see sample knowledge nuggets'
              }
            </p>
            {!searchTerm && selectedCategory === 'ALL' && selectedImpact === 'ALL' && (
              <Button 
                onClick={generateDemoData}
                className="bg-purple-600 hover:bg-purple-700"
              >
                <Lightbulb className="w-4 h-4 mr-2" />
                Generate Demo Nuggets
              </Button>
            )}
          </CardContent>
        </Card>
      )}
    </div>
  )
}

export default KMNuggets

