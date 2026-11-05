FROM oven/bun:1.4.4-slim
WORKDIR /app
COPY src src
COPY public public
ENV DASH_DB=/data/dash.db PORT=8080
EXPOSE 8080
CMD ["bun", "src/dash.ts"]
