FROM node:20-alpine

WORKDIR /app/consumer1

COPY package*.json ./

RUN npm ci

COPY consumer1.js ./

CMD ["node", "consumer1.js"]