# Builds the dashboard and serves it with nginx (see nginx.conf).
FROM node:24.19.0-alpine AS build
WORKDIR /web
COPY package.json package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY . .
RUN npm run build

FROM nginxinc/nginx-unprivileged:1.29.3-alpine
# The entrypoint renders the template into /etc/nginx/conf.d with these
# variables, and with the resolvers of /etc/resolv.conf as
# NGINX_LOCAL_RESOLVERS. Kubernetes sets TW_API_UPSTREAM to the API's
# fully qualified service name: nginx does not apply DNS search domains.
ENV NGINX_ENTRYPOINT_LOCAL_RESOLVERS=1 TW_API_UPSTREAM=http://api:8080
COPY nginx.conf /etc/nginx/templates/default.conf.template
COPY security-headers.conf /etc/nginx/snippets/security-headers.conf
COPY --from=build /web/dist /usr/share/nginx/html
EXPOSE 8080
