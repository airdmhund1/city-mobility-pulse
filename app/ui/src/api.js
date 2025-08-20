const API_BASE = import.meta.env.VITE_API_BASE || "http://localhost:8080";

export async function fetchJson(path) {
  const res = await fetch(`${API_BASE}${path}`);
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return res.json();
}