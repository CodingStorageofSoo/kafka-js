FROM node:20-alpine

WORKDIR /app

COPY package*.json /app

RUN npm ci

COPY producer.js /app

CMD ["node", "producer.js"]