import React, { useEffect, useState } from 'react'

export default function App() {
  const [silverBike, setSilverBike] = useState([])
  const [silverWeather, setSilverWeather] = useState([])
  const [hourly, setHourly] = useState([])
  const [err, setErr] = useState(null)

  useEffect(() => {
    async function load() {
      try {
        const [b, w, h] = await Promise.all([
          fetch('http://localhost:8080/silver/bike?limit=10').then(r => r.json()),
          fetch('http://localhost:8080/silver/weather?limit=10').then(r => r.json()),
          fetch('http://localhost:8080/stations/summary?limit=10').then(r => r.json())
        ])
        setSilverBike(b); setSilverWeather(w); setHourly(h)
      } catch (e) { setErr(String(e)) }
    }
    load()
    const id = setInterval(load, 10000)
    return () => clearInterval(id)
  }, [])

  return (
    <div style={{fontFamily:'Inter,system-ui,sans-serif', padding:24}}>
      <h1>City Mobility Pulse</h1>
      {err && <p style={{color:'crimson'}}>Error: {err}</p>}

      <h2>Silver • Bike (latest)</h2>
      <pre style={{background:'#111', color:'#0f0', padding:12}}>
        {JSON.stringify(silverBike, null, 2)}
      </pre>

      <h2>Silver • Weather (latest)</h2>
      <pre style={{background:'#111', color:'#0f0', padding:12}}>
        {JSON.stringify(silverWeather, null, 2)}
      </pre>

      <h2>Gold • Hourly Summary</h2>
      <pre style={{background:'#111', color:'#0f0', padding:12}}>
        {JSON.stringify(hourly, null, 2)}
      </pre>
    </div>
  )
}
