import React, { useMemo } from 'react';
import './StatsCards.css';
import useFetch from '../../hooks/useFetch';
import { endpoints } from '../../services/api';

export default function StatsCards() {
  const { data: ordersByStatus, loading: loadingOrders, error: errorOrders } =
    useFetch(endpoints.ordersCountByStatus);
  const { data: productsByCategory, loading: loadingProducts, error: errorProducts } =
    useFetch(endpoints.productsCountByCategory);

  const { totalOrders, totalProducts, categoriesCount, topCategory } = useMemo(() => {
    const totals = {
      totalOrders: 0,
      totalProducts: 0,
      categoriesCount: 0,
      topCategory: null,
    };

    if (Array.isArray(ordersByStatus)) {
      totals.totalOrders = ordersByStatus.reduce((sum, o) => sum + (o.count || 0), 0);
    }

    if (Array.isArray(productsByCategory)) {
      // productsByCategory -> [ [category, count], ... ]
      const normalized = productsByCategory.map(([category, count]) => ({
        category,
        count: Number(count || 0),
      }));
      totals.categoriesCount = normalized.length;
      totals.totalProducts = normalized.reduce((sum, p) => sum + p.count, 0);
      if (normalized.length) {
        totals.topCategory = normalized.slice().sort((a, b) => b.count - a.count)[0];
      }
    }
    return totals;
  }, [ordersByStatus, productsByCategory]);

  const isLoading = loadingOrders || loadingProducts;
  const hasError = errorOrders || errorProducts;

  return (
    <section className="stats">
      {isLoading && <div className="stats__info">Loading summary…</div>}
      {hasError && (
        <div className="stats__error">
          Failed to load summary. {errorOrders?.message || errorProducts?.message}
        </div>
      )}
      {!isLoading && !hasError && (
        <div className="stats__grid">
          <div className="stat__card">
            <div className="stat__title">Total Orders</div>
            <div className="stat__value">{totalOrders}</div>
            <div className="stat__hint">Across all statuses</div>
          </div>

          <div className="stat__card">
            <div className="stat__title">Total Products</div>
            <div className="stat__value">{totalProducts}</div>
            <div className="stat__hint">{categoriesCount} categories</div>
          </div>

          <div className="stat__card">
            <div className="stat__title">Top Category</div>
            <div className="stat__value">
              {topCategory ? topCategory.category : '—'}
            </div>
            <div className="stat__hint">
              {topCategory ? `${topCategory.count} items` : 'No data'}
            </div>
          </div>
        </div>
      )}
    </section>
  );
}
