# syntax=docker/dockerfile:1

# Stage 1: Build React SPA assets
FROM node:22-alpine AS builder

WORKDIR /app

# Cache dependencies
COPY package.json package-lock.json ./
RUN npm ci

# Copy source code and compile production bundles
COPY . .
RUN npm run build

# Stage 2: Minimal Nginx Alpine runtime (< 25MB)
FROM nginx:alpine

# Copy custom Nginx configuration with SPA routing and API reverse proxy
COPY nginx.conf /etc/nginx/conf.d/default.conf

# Copy compiled static assets from builder stage
COPY --from=builder /app/dist /usr/share/nginx/html

EXPOSE 3000

CMD ["nginx", "-g", "daemon off;"]
