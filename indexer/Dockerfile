# The Kafka client is a native module, so this service needs a real build
# rather than the volume-mounted approach the generator uses.
FROM node:24-slim

WORKDIR /app

# npm ci installs exactly what the lockfile pins, so rebuilds are reproducible.
COPY package.json package-lock.json ./
RUN npm ci --omit=dev

COPY tsconfig.json ./
COPY src ./src

CMD ["node", "src/index.ts"]
