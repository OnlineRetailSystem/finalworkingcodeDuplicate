import React, { useMemo } from 'react';
import './ProductsByCategoryBar.css';
import useFetch from '../../../hooks/useFetch';
import { endpoints } from '../../../services/api';
import {
  BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid,
} from 'recharts';

export default function ProductsByCategoryBar() {
  const { data, loading, error } = useFetch(endpoints.productsCountByCategory);

  const chartData = useMemo(() => {
    if (!Array.isArray(data)) return [];
    // API returns [ [category, count], ... ]
    return data.map((row) => {
      const [category, count] = row;
      return { category: String(category ?? 'Unknown'), count: Number(count || 0) };
    });
  }, [data]);

  return (
    <section className="panel">
      <div className="panel__header">
        <h3>Products by Category</h3>
      </div>

      {loading && <div className="panel__info">Loading bar chart…</div>}
      {error && <div className="panel__error">Failed: {error.message}</div>}
      {!loading && !error && chartData.length === 0 && (
        <div className="panel__info">No data available</div>
      )}

      {!loading && !error && chartData.length > 0 && (
        <div className="panel__body">
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={chartData} margin={{ top: 10, right: 10, left: 0, bottom: 20 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" />
              <XAxis
                dataKey="category"
                tick={{ fill: '#9ca3af', fontSize: 12 }}
                interval={0}
                angle={-20}
                textAnchor="end"
                height={50}
              />
              <YAxis tick={{ fill: '#9ca3af', fontSize: 12 }} />
              <Tooltip
                contentStyle={{ background: '#0b1220', border: '1px solid #1f2937', color: '#e5e7eb' }}
                cursor={{ fill: 'rgba(96,165,250,0.08)' }}
              />
              <Bar dataKey="count" fill="#60a5fa" radius={[6, 6, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      )}
    </section>
  );
}
