FROM oven/bun:1.4.4-slim@sha256:1846adf22496bd711036aae8be147e57bdebbaea2e5264e3e0b5af1db187d9a4
WORKDIR /app
COPY src src
COPY public public
ENV DASH_DB=/data/dash.db PORT=8080 NODE_ENV=production
# unprivileged: the app only needs to read /app and write /data
USER bun
EXPOSE 8080
CMD ["bun", "src/dash.ts"]
