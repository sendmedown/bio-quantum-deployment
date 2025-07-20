import { Link, useLocation } from 'react-router-dom'
import { 
  BarChart3, 
  Briefcase, 
  TrendingUp, 
  Brain, 
  Activity,
  Wifi,
  WifiOff
} from 'lucide-react'
import { Button } from '@/components/ui/button'

const Navigation = ({ isConnected }) => {
  const location = useLocation()

  const navItems = [
    { path: '/', label: 'Dashboard', icon: BarChart3 },
    { path: '/portfolio', label: 'Portfolio', icon: Briefcase },
    { path: '/signals', label: 'Signals', icon: TrendingUp },
    { path: '/knowledge', label: 'KM Nuggets', icon: Brain },
    { path: '/analytics', label: 'Analytics', icon: Activity },
  ]

  return (
    <nav className="bg-slate-800/50 backdrop-blur-sm border-b border-purple-500/20">
      <div className="container mx-auto px-4">
        <div className="flex items-center justify-between h-16">
          {/* Logo and Title */}
          <div className="flex items-center space-x-3">
            <div className="w-8 h-8 bg-gradient-to-r from-purple-500 to-cyan-500 rounded-lg flex items-center justify-center">
              <Activity className="w-5 h-5 text-white" />
            </div>
            <h1 className="text-xl font-bold bg-gradient-to-r from-purple-400 to-cyan-400 bg-clip-text text-transparent">
              Bio-Quantum AI Trading
            </h1>
          </div>

          {/* Navigation Links */}
          <div className="hidden md:flex items-center space-x-1">
            {navItems.map((item) => {
              const Icon = item.icon
              const isActive = location.pathname === item.path
              
              return (
                <Link key={item.path} to={item.path}>
                  <Button
                    variant={isActive ? "default" : "ghost"}
                    size="sm"
                    className={`flex items-center space-x-2 ${
                      isActive 
                        ? 'bg-purple-600 hover:bg-purple-700' 
                        : 'text-gray-300 hover:text-white hover:bg-slate-700'
                    }`}
                  >
                    <Icon className="w-4 h-4" />
                    <span>{item.label}</span>
                  </Button>
                </Link>
              )
            })}
          </div>

          {/* Connection Status */}
          <div className="flex items-center space-x-2">
            <div className={`flex items-center space-x-2 px-3 py-1 rounded-full text-sm ${
              isConnected 
                ? 'bg-green-500/20 text-green-400' 
                : 'bg-red-500/20 text-red-400'
            }`}>
              {isConnected ? (
                <>
                  <Wifi className="w-4 h-4" />
                  <span>Connected</span>
                </>
              ) : (
                <>
                  <WifiOff className="w-4 h-4" />
                  <span>Disconnected</span>
                </>
              )}
            </div>
          </div>
        </div>

        {/* Mobile Navigation */}
        <div className="md:hidden pb-4">
          <div className="flex flex-wrap gap-2">
            {navItems.map((item) => {
              const Icon = item.icon
              const isActive = location.pathname === item.path
              
              return (
                <Link key={item.path} to={item.path}>
                  <Button
                    variant={isActive ? "default" : "ghost"}
                    size="sm"
                    className={`flex items-center space-x-2 ${
                      isActive 
                        ? 'bg-purple-600 hover:bg-purple-700' 
                        : 'text-gray-300 hover:text-white hover:bg-slate-700'
                    }`}
                  >
                    <Icon className="w-4 h-4" />
                    <span>{item.label}</span>
                  </Button>
                </Link>
              )
            })}
          </div>
        </div>
      </div>
    </nav>
  )
}

export default Navigation

