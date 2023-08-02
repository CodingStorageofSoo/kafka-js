FROM node:20-alpine

WORKDIR /app/producer

COPY package*.json ./

RUN npm ci

COPY producer.js ./

CMD ["node", "producer.js"]