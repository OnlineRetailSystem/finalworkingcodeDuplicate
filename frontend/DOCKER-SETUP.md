# Frontend Docker Setup Guide

## Overview

The frontend is a React application built with Vite and served using Nginx in production.

## Docker Configuration

### Multi-Stage Build

The Dockerfile uses a two-stage build process:

1. **Build Stage**: Compiles React app using Node.js
2. **Runtime Stage**: Serves static files using Nginx

### Image Sizes
- Build stage: ~1.2GB (Node.js + dependencies)
- Runtime stage: ~25MB (Nginx + static files)
- Final image: ~25MB

## Features

### Nginx Configuration
- ✅ Gzip compression enabled
- ✅ Security headers configured
- ✅ Static asset caching (1 year)
- ✅ SPA routing support
- ✅ Health check endpoint
- ✅ Optional API proxy support

### Health Checks
- Endpoint: `/health`
- Interval: 30 seconds
- Timeout: 3 seconds
- Retries: 3

## Building the Image

### Local Build
```bash
cd frontend
docker build -t ecom-frontend .
```

### Via Docker Compose
```bash
cd "Ecom-Backend-Final 4/Ecom-Backend-Final"
docker-compose build frontend
```

## Running the Container

### Standalone
```bash
docker run -d -p 3000:80 --name ecom-frontend ecom-frontend
```

### With Docker Compose
```bash
docker-compose up -d frontend
```

## Environment Variables

### Development (.env.development)
```env
VITE_AUTH_SERVICE_URL=http://localhost:8087
VITE_PRODUCT_SERVICE_URL=http://localhost:8082
VITE_ORDER_SERVICE_URL=http://localhost:8083
VITE_CART_SERVICE_URL=http://localhost:8084
VITE_PAYMENT_SERVICE_URL=http://localhost:8088
```

### Production (.env.production)
```env
VITE_AUTH_SERVICE_URL=http://localhost:8090/authservice
VITE_PRODUCT_SERVICE_URL=http://localhost:8090/productservice
VITE_ORDER_SERVICE_URL=http://localhost:8090/orderservice
VITE_CART_SERVICE_URL=http://localhost:8090/cartservice
VITE_PAYMENT_SERVICE_URL=http://localhost:8090/paymentservice
```

### Custom Environment
Create `.env.local` (not committed to git):
```env
VITE_AUTH_SERVICE_URL=http://your-custom-url:8087
```

## Accessing the Application

### Local Development
```bash
npm run dev
```
Access at: http://localhost:5173

### Docker Container
```bash
docker-compose up -d frontend
```
Access at: http://localhost:3000

## Nginx Configuration

### Default Configuration
Located at: `nginx.conf`

Key features:
- Serves from `/usr/share/nginx/html`
- SPA routing (all routes → index.html)
- Gzip compression
- Security headers
- Static asset caching

### Custom Configuration
To modify Nginx config:

1. Edit `nginx.conf`
2. Rebuild image:
   ```bash
   docker-compose build frontend
   docker-compose up -d frontend
   ```

### API Proxy (Optional)

To proxy API calls through Nginx, uncomment in `nginx.conf`:

```nginx
location /api/ {
    proxy_pass http://apigateway:8090/;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection 'upgrade';
    proxy_set_header Host $host;
    proxy_cache_bypass $http_upgrade;
}
```

Then update frontend to use relative URLs:
```javascript
// Instead of: http://localhost:8090/auth/login
// Use: /api/auth/login
```

## Troubleshooting

### Issue: Build fails with "npm install" errors
**Solution**: Clear npm cache and rebuild
```bash
docker-compose build --no-cache frontend
```

### Issue: Page shows "Cannot GET /"
**Solution**: Check Nginx configuration for SPA routing
```nginx
location / {
    try_files $uri $uri/ /index.html;
}
```

### Issue: API calls fail with CORS errors
**Solution**: 
1. Check backend CORS configuration
2. Or use Nginx proxy (see above)

### Issue: Static assets not loading
**Solution**: Check build output
```bash
docker-compose exec frontend ls -la /usr/share/nginx/html
```

### Issue: Changes not reflected
**Solution**: Rebuild without cache
```bash
docker-compose build --no-cache frontend
docker-compose up -d frontend
```

## Performance Optimization

### Build Optimization
- Uses `npm ci` for faster, deterministic installs
- Multi-stage build reduces final image size
- Only production dependencies in final image

### Runtime Optimization
- Gzip compression (reduces transfer size by ~70%)
- Static asset caching (1 year)
- Nginx serves files directly (no Node.js overhead)

### Network Optimization
```nginx
# Already configured in nginx.conf
gzip on;
gzip_types text/plain text/css application/json application/javascript;
```

## Security

### Headers Configured
```nginx
X-Frame-Options: SAMEORIGIN
X-Content-Type-Options: nosniff
X-XSS-Protection: 1; mode=block
```

### Best Practices
- ✅ No sensitive data in environment variables
- ✅ Static files only (no server-side code)
- ✅ Security headers enabled
- ✅ Minimal attack surface (Alpine Linux)

### For Production
Additional recommendations:
- Enable HTTPS
- Add Content Security Policy
- Implement rate limiting
- Use CDN for static assets

## Monitoring

### Health Check
```bash
curl http://localhost:3000/health
```
Expected: `OK`

### Container Logs
```bash
docker-compose logs -f frontend
```

### Nginx Access Logs
```bash
docker-compose exec frontend tail -f /var/log/nginx/access.log
```

### Nginx Error Logs
```bash
docker-compose exec frontend tail -f /var/log/nginx/error.log
```

## Development Workflow

### 1. Local Development
```bash
cd frontend
npm install
npm run dev
```

### 2. Test Build
```bash
npm run build
npm run preview
```

### 3. Docker Build
```bash
docker-compose build frontend
```

### 4. Deploy
```bash
docker-compose up -d frontend
```

## File Structure

```
frontend/
├── public/              # Static assets
├── src/                 # Source code
│   ├── components/      # React components
│   ├── config/          # Configuration files
│   │   └── api.config.js # API URLs
│   ├── Page/            # Page components
│   └── services/        # API services
├── .dockerignore        # Docker ignore rules
├── .env.development     # Dev environment
├── .env.production      # Prod environment
├── .env.example         # Example config
├── Dockerfile           # Docker build instructions
├── nginx.conf           # Nginx configuration
├── package.json         # Dependencies
└── vite.config.js       # Vite configuration
```

## Updating Dependencies

### Check for Updates
```bash
npm outdated
```

### Update Dependencies
```bash
npm update
```

### Rebuild Image
```bash
docker-compose build --no-cache frontend
```

## Cleanup

### Remove Container
```bash
docker-compose stop frontend
docker-compose rm frontend
```

### Remove Image
```bash
docker rmi ecom-backend-final-frontend
```

### Full Cleanup
```bash
docker-compose down
docker system prune -a
```

## CI/CD Integration

### Build Command
```bash
docker build -t ecom-frontend:${VERSION} .
```

### Test Command
```bash
docker run --rm ecom-frontend:${VERSION} curl -f http://localhost/health
```

### Push Command
```bash
docker tag ecom-frontend:${VERSION} registry/ecom-frontend:${VERSION}
docker push registry/ecom-frontend:${VERSION}
```

## Support

For issues:
1. Check logs: `docker-compose logs frontend`
2. Verify health: `curl http://localhost:3000/health`
3. Check Nginx config: `docker-compose exec frontend cat /etc/nginx/conf.d/default.conf`
4. Review build output: `docker-compose build frontend`

## Additional Resources

- [Vite Documentation](https://vitejs.dev/)
- [Nginx Documentation](https://nginx.org/en/docs/)
- [Docker Best Practices](https://docs.docker.com/develop/dev-best-practices/)
- [React Documentation](https://react.dev/)
