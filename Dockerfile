#
# Build
#
ARG NODE_VERSION=22.8-bullseye
FROM node:${NODE_VERSION} AS build
ENV PUPPETEER_SKIP_DOWNLOAD=True

# npm packages
WORKDIR /src
COPY package.json .
COPY yarn.lock .
ENV YARN_CHECKSUM_BEHAVIOR=update
RUN npm install -g corepack && corepack enable
RUN yarn set version 3.6.3
RUN yarn install

# App
WORKDIR /src
ADD . /src
RUN yarn install
RUN yarn build && find ./dist -name "*.d.ts" -delete

#
# Dashboard
#
FROM node:${NODE_VERSION} AS dashboard

# Download WAHA Dashboard
ENV WAHA_DASHBOARD_SHA fed4e50e88e4d26c610e3289fd0d8657cb866543
RUN \
    wget https://github.com/devlikeapro/dashboard/archive/${WAHA_DASHBOARD_SHA}.zip \
    && unzip ${WAHA_DASHBOARD_SHA}.zip -d /tmp/dashboard \
    && mkdir -p /dashboard \
    && mv /tmp/dashboard/dashboard-${WAHA_DASHBOARD_SHA}/* /dashboard/ \
    && rm -rf ${WAHA_DASHBOARD_SHA}.zip \
    && rm -rf /tmp/dashboard/dashboard-${WAHA_DASHBOARD_SHA}

#
# Final
#
FROM node:${NODE_VERSION} AS release
ENV PUPPETEER_SKIP_DOWNLOAD=True
# Quick fix for memory potential memory leaks
# https://github.com/devlikeapro/waha/issues/347
ENV NODE_OPTIONS="--max-old-space-size=16384"
ARG USE_BROWSER=chromium
ARG WHATSAPP_DEFAULT_ENGINE

RUN echo "USE_BROWSER=$USE_BROWSER"

# Install ffmpeg to generate previews for videos
RUN apt-get update && apt-get install -y ffmpeg --no-install-recommends && rm -rf /var/lib/apt/lists/*

# Install zip and unzip - either for chromium or chrome
RUN if [ "$USE_BROWSER" = "chromium" ] || [ "$USE_BROWSER" = "chrome" ]; then \
    apt-get update  \
    && apt-get install -y zip unzip \
    && rm -rf /var/lib/apt/lists/*; \
    fi

# Install fonts if using either chromium or chrome
RUN if [ "$USE_BROWSER" = "chromium" ] || [ "$USE_BROWSER" = "chrome" ]; then \
    apt-get update  \
    && apt-get install -y \
        fontconfig \
        fonts-freefont-ttf \
        fonts-gfs-neohellenic \
        fonts-indic \
        fonts-ipafont-gothic \
        fonts-kacst \
        fonts-liberation \
        fonts-noto-cjk \
        fonts-noto-color-emoji \
        fonts-roboto \
        fonts-thai-tlwg \
        fonts-wqy-zenhei \
        fonts-open-sans \
      --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*; \
    fi

# Install Chromium
RUN if [ "$USE_BROWSER" = "chromium" ]; then \
        apt-get update  \
        && apt-get update \
        && apt-get install -y chromium \
          --no-install-recommends \
        && rm -rf /var/lib/apt/lists/*; \
    fi

# Install Chrome
# Available versions:
# https://www.ubuntuupdates.org/package/google_chrome/stable/main/base/google-chrome-stable
ARG CHROME_VERSION="130.0.6723.69-1"
RUN if [ "$USE_BROWSER" = "chrome" ]; then \
        wget --no-verbose -O /tmp/chrome.deb https://dl.google.com/linux/chrome/deb/pool/main/g/google-chrome-stable/google-chrome-stable_${CHROME_VERSION}_amd64.deb \
          && apt-get update \
          && apt install -y /tmp/chrome.deb \
          && rm /tmp/chrome.deb \
          && rm -rf /var/lib/apt/lists/*; \
    fi

# Set the ENV for NOWEB docker image
ENV WHATSAPP_DEFAULT_ENGINE=$WHATSAPP_DEFAULT_ENGINE

# Attach sources, install packages
WORKDIR /app
COPY package.json ./
COPY --from=build /src/node_modules ./node_modules
COPY --from=build /src/dist ./dist
COPY --from=dashboard /dashboard ./dist/dashboard
COPY entrypoint.sh /entrypoint.sh

# Chokidar options to monitor file changes
ENV CHOKIDAR_USEPOLLING=1
ENV CHOKIDAR_INTERVAL=5000

# WAHA variables
ENV WAHA_ZIPPER=ZIPUNZIP

# Run command, etc
EXPOSE 3000
CMD ["/entrypoint.sh"]
