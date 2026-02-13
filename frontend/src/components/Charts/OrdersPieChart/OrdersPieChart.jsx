import React, { useMemo } from 'react';
import './OrdersPieChart.css';
import useFetch from '../../../hooks/useFetch';
import { endpoints } from '../../../services/api';
import { PieChart, Pie, Cell, Tooltip, Legend, ResponsiveContainer } from 'recharts';

const COLORS = ['#60a5fa', '#22c55e', '#f59e0b', '#ef4444', '#a78bfa', '#f472b6'];

export default function OrdersPieChart() {
  const { data, loading, error } = useFetch(endpoints.ordersCountByStatus);

  const chartData = useMemo(() => {
    if (!Array.isArray(data)) return [];
    // Normalize to { name, value }
    return data.map((d) => ({
      name: d.status ?? 'Unknown',
      value: Number(d.count || 0),
    }));
  }, [data]);

  return (
    <section className="panel">
      <div className="panel__header">
        <h3>Orders by Status</h3>
      </div>

      {loading && <div className="panel__info">Loading pie chart…</div>}
      {error && <div className="panel__error">Failed: {error.message}</div>}
      {!loading && !error && chartData.length === 0 && (
        <div className="panel__info">No data available</div>
      )}

      {!loading && !error && chartData.length > 0 && (
        <div className="panel__body">
          <ResponsiveContainer width="100%" height={280}>
            <PieChart>
              <Pie
                data={chartData}
                dataKey="value"
                nameKey="name"
                cx="50%"
                cy="50%"
                outerRadius={100}
                label
              >
                {chartData.map((entry, index) => (
                  <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                ))}
              </Pie>
              <Tooltip />
              <Legend />
            </PieChart>
          </ResponsiveContainer>
        </div>
      )}
    </section>
  );
}
