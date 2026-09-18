FROM node:20-alpine

WORKDIR /app

COPY package*.json ./
RUN npm install --omit=dev

COPY src ./src

# Overridden per service in docker-compose.yml
CMD ["node", "src/api/server.js"]
