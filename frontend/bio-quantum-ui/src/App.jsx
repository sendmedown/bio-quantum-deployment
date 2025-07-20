import { useState, useEffect } from 'react'
import { BrowserRouter as Router, Routes, Route } from 'react-router-dom'
import { io } from 'socket.io-client'
import Dashboard from './components/Dashboard'
import Portfolio from './components/Portfolio'
import Signals from './components/Signals'
import KMNuggets from './components/KMNuggets'
import Analytics from './components/Analytics'
import Navigation from './components/Navigation'
import { WebSocketProvider } from './hooks/useWebSocket'
import './App.css'

function App() {
  const [socket, setSocket] = useState(null)
  const [isConnected, setIsConnected] = useState(false)

  useEffect(() => {
    // Initialize WebSocket connection
    const newSocket = io('http://localhost:5000', {
      transports: ['websocket', 'polling']
    })

    newSocket.on('connect', () => {
      console.log('Connected to Bio-Quantum Trading Platform')
      setIsConnected(true)
    })

    newSocket.on('disconnect', () => {
      console.log('Disconnected from Bio-Quantum Trading Platform')
      setIsConnected(false)
    })

    newSocket.on('connection_status', (data) => {
      console.log('Connection status:', data)
    })

    setSocket(newSocket)

    return () => {
      newSocket.close()
    }
  }, [])

  return (
    <WebSocketProvider socket={socket}>
      <Router>
        <div className="min-h-screen bg-gradient-to-br from-slate-900 via-purple-900 to-slate-900">
          <Navigation isConnected={isConnected} />
          
          <main className="container mx-auto px-4 py-6">
            <Routes>
              <Route path="/" element={<Dashboard />} />
              <Route path="/portfolio" element={<Portfolio />} />
              <Route path="/signals" element={<Signals />} />
              <Route path="/knowledge" element={<KMNuggets />} />
              <Route path="/analytics" element={<Analytics />} />
            </Routes>
          </main>
        </div>
      </Router>
    </WebSocketProvider>
  )
}

export default App

