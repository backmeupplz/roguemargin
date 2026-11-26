FROM oven/bun:1.4.5-slim@sha256:b780593f3e10918cbdaec3d5a08d52224b9198069a65eb47812085c2ec007643
# zlib1g: the base image's is older than Debian's fix for CVE-2026-85091
RUN apt-get update && apt-get install -y --no-install-recommends --only-upgrade zlib1g && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY src src
COPY public public
ENV DASH_DB=/data/dash.db PORT=8080 NODE_ENV=production
# unprivileged: the app only needs to read /app and write /data
USER bun
EXPOSE 8080
CMD ["bun", "src/dash.ts"]
