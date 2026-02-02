FROM node:23.11.1-alpine

RUN apk add --no-cache \
    postgresql16-client \
    zstd \
    git

WORKDIR /app

COPY package.json yarn.lock* ./

RUN yarn install --frozen-lockfile

COPY backup.js ./

ENTRYPOINT ["yarn","run", "backup-zstd"]
