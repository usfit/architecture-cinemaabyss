FROM node:20-alpine

WORKDIR /app

COPY package*.json ./

RUN npm ci

COPY main.js ./

EXPOSE 8082

CMD ["node", "main.js"]