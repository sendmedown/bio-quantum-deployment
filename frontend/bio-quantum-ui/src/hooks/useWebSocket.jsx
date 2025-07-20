import { createContext, useContext, useEffect, useState } from 'react'

const WebSocketContext = createContext()

export const useWebSocket = () => {
  const context = useContext(WebSocketContext)
  if (!context) {
    throw new Error('useWebSocket must be used within a WebSocketProvider')
  }
  return context
}

export const WebSocketProvider = ({ children, socket }) => {
  const [marketData, setMarketData] = useState({})
  const [tradingSignals, setTradingSignals] = useState([])
  const [quantumMetrics, setQuantumMetrics] = useState({})
  const [alerts, setAlerts] = useState([])

  useEffect(() => {
    if (!socket) return

    // Market data updates
    socket.on('market_data_update', (data) => {
      setMarketData(prev => ({
        ...prev,
        [data.symbol]: data
      }))
    })

    // Trading signal updates
    socket.on('trading_signal_update', (signal) => {
      setTradingSignals(prev => [signal, ...prev.slice(0, 19)]) // Keep last 20 signals
    })

    // Quantum metrics updates
    socket.on('quantum_metrics_update', (metrics) => {
      setQuantumMetrics(metrics)
    })

    // Platform alerts
    socket.on('platform_alert', (alert) => {
      setAlerts(prev => [alert, ...prev.slice(0, 9)]) // Keep last 10 alerts
    })

    // Subscribe to real-time data
    socket.emit('subscribe_market_data', { symbols: ['BTC', 'ETH', 'AAPL', 'GOOGL', 'TSLA', 'NVDA', 'AMZN', 'MSFT'] })
    socket.emit('subscribe_signals')
    socket.emit('subscribe_quantum_metrics')

    return () => {
      socket.off('market_data_update')
      socket.off('trading_signal_update')
      socket.off('quantum_metrics_update')
      socket.off('platform_alert')
    }
  }, [socket])

  const subscribeToMarketData = (symbols) => {
    if (socket) {
      socket.emit('subscribe_market_data', { symbols })
    }
  }

  const subscribeToSignals = () => {
    if (socket) {
      socket.emit('subscribe_signals')
    }
  }

  const subscribeToQuantumMetrics = () => {
    if (socket) {
      socket.emit('subscribe_quantum_metrics')
    }
  }

  const value = {
    socket,
    marketData,
    tradingSignals,
    quantumMetrics,
    alerts,
    subscribeToMarketData,
    subscribeToSignals,
    subscribeToQuantumMetrics
  }

  return (
    <WebSocketContext.Provider value={value}>
      {children}
    </WebSocketContext.Provider>
  )
}

