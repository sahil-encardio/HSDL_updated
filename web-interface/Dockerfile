FROM node:18-alpine

WORKDIR /app

# Copiar package.json e instalar dependencias
COPY package.json ./
RUN npm install --production

# Copiar archivos de la aplicación
COPY server.js ./
COPY index.html ./

EXPOSE 8080

CMD ["node", "server.js"]