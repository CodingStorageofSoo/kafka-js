FROM node:20-alpine

WORKDIR /app/consumer2

COPY package*.json ./

RUN npm ci

COPY consumer2.js ./

CMD ["node", "consumer2.js"]