// API Configuration
// Uses environment variables with fallback to localhost

const config = {
  AUTH_SERVICE_URL: import.meta.env.VITE_AUTH_SERVICE_URL || 'http://localhost:8087',
  PRODUCT_SERVICE_URL: import.meta.env.VITE_PRODUCT_SERVICE_URL || 'http://localhost:8082',
  ORDER_SERVICE_URL: import.meta.env.VITE_ORDER_SERVICE_URL || 'http://localhost:8083',
  CART_SERVICE_URL: import.meta.env.VITE_CART_SERVICE_URL || 'http://localhost:8084',
  PAYMENT_SERVICE_URL: import.meta.env.VITE_PAYMENT_SERVICE_URL || 'http://localhost:8088',
  CATEGORY_SERVICE_URL: import.meta.env.VITE_CATEGORY_SERVICE_URL || 'http://localhost:8085',
  USER_SERVICE_URL: import.meta.env.VITE_USER_SERVICE_URL || 'http://localhost:8081',
};

// Export individual URLs
export const AUTH_SERVICE_URL = config.AUTH_SERVICE_URL;
export const PRODUCT_SERVICE_URL = config.PRODUCT_SERVICE_URL;
export const ORDER_SERVICE_URL = config.ORDER_SERVICE_URL;
export const CART_SERVICE_URL = config.CART_SERVICE_URL;
export const PAYMENT_SERVICE_URL = config.PAYMENT_SERVICE_URL;
export const CATEGORY_SERVICE_URL = config.CATEGORY_SERVICE_URL;
export const USER_SERVICE_URL = config.USER_SERVICE_URL;

// Export default config object
export default config;

// Helper function to build full URL
export const buildUrl = (service, path) => {
  const baseUrl = config[`${service.toUpperCase()}_SERVICE_URL`];
  if (!baseUrl) {
    console.warn(`Service ${service} not configured, using localhost`);
    return `http://localhost:8080${path}`;
  }
  return `${baseUrl}${path}`;
};

// Log configuration in development
if (import.meta.env.DEV) {
  console.log('API Configuration:', config);
}
