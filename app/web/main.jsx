import React from 'react'
import { createRoot } from 'react-dom/client'
import axios from 'axios'

const API = import.meta.env.VITE_API_URL || 'http://localhost:8080'

function App(){
  const [rows,setRows] = React.useState([])
  React.useEffect(()=>{
    axios.get(`${API}/stations/summary`).then(r=>setRows(r.data))
  },[])
  return (
    <div style={{fontFamily:"Inter, system-ui", padding:24}}>
      <h2>City Mobility Pulse</h2>
      <p>Demo aggregates (replace with live Gold table shortly)</p>
      <ul>
        {rows.map((r,i)=><li key={i}>{r.station_id}: bikes {r.avg_bikes} / docks {r.avg_docks}</li>)}
      </ul>
    </div>
  )
}
createRoot(document.getElementById('root')).render(<App/>)
