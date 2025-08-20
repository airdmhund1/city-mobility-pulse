import React from 'react'
import { createRoot } from 'react-dom/client'
import axios from 'axios'

const API = import.meta.env.VITE_API_URL || 'http://localhost:8080'

function App(){
  const [rows,setRows] = React.useState([])
  React.useEffect(()=>{
    axios.get(`${API}/stations/summary`)
      .then(r => { console.log("API /stations/summary raw:", r.data); setRows(r.data); })
      .catch(err => {
        console.error("UI fetch error:", err?.response?.status, err?.message);
      });
  },[])
  return (
    <div style={{fontFamily:"Inter, system-ui", padding:24}}>
      <h2>City Mobility Pulse</h2>
      <p>Demo aggregates (replace with live Gold tables shortly)</p>
      <ul>
        {rows.map((r,i)=>(
          <li key={i}>
            <code>{JSON.stringify(r)}</code>
          </li>
        ))}
      </ul>
    </div>
  )
}
createRoot(document.getElementById('root')).render(<App/>)
