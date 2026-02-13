# Frontend API Configuration Migration Guide

## Overview

The frontend has been updated to use centralized API configuration instead of hardcoded URLs throughout the codebase.

## What Changed

### Before
```javascript
const res = await fetch("http://localhost:8087/signin", { ... });
```

### After
```javascript
import { AUTH_SERVICE_URL } from '../config/api.config';
const res = await fetch(`${AUTH_SERVICE_URL}/signin`, { ... });
```

## Configuration Files

### 1. `.env.development` (Local Development)
```env
VITE_AUTH_SERVICE_URL=http://localhost:8087
VITE_PRODUCT_SERVICE_URL=http://localhost:8082
VITE_ORDER_SERVICE_URL=http://localhost:8083
VITE_CART_SERVICE_URL=http://localhost:8084
VITE_PAYMENT_SERVICE_URL=http://localhost:8088
```

### 2. `.env.production` (Docker/Production)
```env
VITE_AUTH_SERVICE_URL=http://localhost:8090/authservice
VITE_PRODUCT_SERVICE_URL=http://localhost:8090/productservice
VITE_ORDER_SERVICE_URL=http://localhost:8090/orderservice
VITE_CART_SERVICE_URL=http://localhost:8090/cartservice
VITE_PAYMENT_SERVICE_URL=http://localhost:8090/paymentservice
```

### 3. `src/config/api.config.js` (Central Configuration)
Exports all service URLs with fallbacks.

## Migration Steps for Existing Code

### Step 1: Import the configuration
```javascript
import { 
  AUTH_SERVICE_URL, 
  PRODUCT_SERVICE_URL, 
  ORDER_SERVICE_URL,
  CART_SERVICE_URL,
  PAYMENT_SERVICE_URL 
} from '../config/api.config';
```

### Step 2: Replace hardcoded URLs

**Before:**
```javascript
fetch("http://localhost:8087/signin", { ... })
```

**After:**
```javascript
fetch(`${AUTH_SERVICE_URL}/signin`, { ... })
```

## Service URL Mapping

| Service | Environment Variable | Default Value |
|---------|---------------------|---------------|
| Auth | `VITE_AUTH_SERVICE_URL` | http://localhost:8087 |
| Product | `VITE_PRODUCT_SERVICE_URL` | http://localhost:8082 |
| Order | `VITE_ORDER_SERVICE_URL` | http://localhost:8083 |
| Cart | `VITE_CART_SERVICE_URL` | http://localhost:8084 |
| Payment | `VITE_PAYMENT_SERVICE_URL` | http://localhost:8088 |
| Category | `VITE_CATEGORY_SERVICE_URL` | http://localhost:8085 |
| User | `VITE_USER_SERVICE_URL` | http://localhost:8081 |

## Files That Need Migration

The following files still have hardcoded URLs and should be updated:

1. `src/Page/pages/CartPage.jsx`
2. `src/components/SignUp/SignUp.jsx`
3. `src/components/SignIn/SignIn.jsx`
4. `src/components/SearchResult/SearchResult.jsx`
5. `src/components/ProfileEdit/ProfileEdit.jsx`
6. `src/components/Profile/Profile.jsx`
7. `src/components/ProductDetails/ProductDetails.jsx`
8. `src/components/ProductCategorySlider/ProductCategorySlider.jsx`
9. `src/components/ProductCategoryByProductId/ProductCategoryByProductId.jsx`
10. `src/components/Payment/PaymentPage.jsx`
11. `src/components/OrderHistory/OrderHistory.jsx`
12. `src/components/EditProduct/EditProduct.jsx`
13. `src/components/comp/BillingInfo.jsx`
14. `src/components/Cart/Cart.jsx`

## Example Migrations

### Example 1: SignIn Component

**Before:**
```javascript
const res = await fetch("http://localhost:8087/signin", {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify({ username, password })
});
```

**After:**
```javascript
import { AUTH_SERVICE_URL } from '../../config/api.config';

const res = await fetch(`${AUTH_SERVICE_URL}/signin`, {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify({ username, password })
});
```

### Example 2: Cart Component

**Before:**
```javascript
const res = await fetch(`http://localhost:8084/cart/${username}`);
await fetch("http://localhost:8084/cart/add", { ... });
await fetch(`http://localhost:8084/cart/${item.id}`, { method: "DELETE" });
```

**After:**
```javascript
import { CART_SERVICE_URL } from '../../config/api.config';

const res = await fetch(`${CART_SERVICE_URL}/cart/${username}`);
await fetch(`${CART_SERVICE_URL}/cart/add`, { ... });
await fetch(`${CART_SERVICE_URL}/cart/${item.id}`, { method: "DELETE" });
```

### Example 3: Product Listing

**Before:**
```javascript
const res = await fetch("http://localhost:8082/products");
```

**After:**
```javascript
import { PRODUCT_SERVICE_URL } from '../../config/api.config';

const res = await fetch(`${PRODUCT_SERVICE_URL}/products`);
```

## Testing

### Local Development
```bash
npm run dev
```
Uses `.env.development` - connects directly to services

### Production Build
```bash
npm run build
```
Uses `.env.production` - connects via API Gateway

### Docker Build
The Dockerfile automatically uses production environment variables.

## Benefits

1. **Single Source of Truth**: All API URLs in one place
2. **Environment-Specific**: Different URLs for dev/prod
3. **Easy Updates**: Change one file instead of 50+ locations
4. **Type Safety**: Centralized imports reduce typos
5. **Docker Ready**: Works seamlessly in containers

## Troubleshooting

### Issue: API calls failing after migration
**Solution**: Check that environment variables are loaded
```javascript
console.log(import.meta.env);
```

### Issue: CORS errors
**Solution**: Ensure backend services allow frontend origin
```javascript
// Backend should allow: http://localhost:3000
```

### Issue: 404 errors via API Gateway
**Solution**: Verify API Gateway routes are configured correctly

## Next Steps

1. Migrate all components to use centralized config
2. Remove all hardcoded URLs
3. Test in both development and production modes
4. Update API Gateway routes if needed
5. Document any custom endpoints

## Support

For issues or questions:
1. Check `src/config/api.config.js` for available URLs
2. Verify `.env.development` or `.env.production` settings
3. Check browser console for configuration logs (dev mode)
