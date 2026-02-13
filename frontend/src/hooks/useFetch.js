// src/hooks/useFetch.js
import { useEffect, useState } from 'react';

export default function useFetch(url, { immediate = true, options = {} } = {}) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(Boolean(immediate));
  const [error, setError] = useState(null);

  useEffect(() => {
    if (!immediate || !url) return;

    const controller = new AbortController();
    const { signal } = controller;

    async function run() {
      setLoading(true);
      setError(null);
      try {
        const res = await fetch(url, { ...options, signal });
        if (!res.ok) {
          throw new Error(`HTTP ${res.status} ${res.statusText}`);
        }
        const json = await res.json();
        setData(json);
      } catch (err) {
        if (err.name !== 'AbortError') {
          setError(err);
        }
      } finally {
        setLoading(false);
      }
    }

    run();
    return () => controller.abort();
  }, [url]);

  return { data, loading, error };
}
