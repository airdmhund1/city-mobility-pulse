FROM node:20-alpine
WORKDIR /web
COPY app/web/package*.json ./
RUN npm ci
COPY app/web .
EXPOSE 5173
CMD ["npm","run","dev","--","--host","0.0.0.0"]
