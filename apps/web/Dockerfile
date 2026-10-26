# syntax=docker/dockerfile:1
FROM node:24-alpine AS build

WORKDIR /app
COPY apps/web/package.json apps/web/package-lock.json ./

ARG NPM_CONFIG_STRICT_SSL=true
RUN if [ "$NPM_CONFIG_STRICT_SSL" = "false" ]; then \
      echo "WARNING: npm TLS certificate verification is disabled"; \
    fi && \
    npm config set strict-ssl "$NPM_CONFIG_STRICT_SSL" && \
    npm ci

COPY apps/web/ ./
RUN npm run build

FROM nginx:1.29-alpine
COPY apps/web/nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/dist /usr/share/nginx/html
EXPOSE 80
