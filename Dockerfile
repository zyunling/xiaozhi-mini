FROM node:22-alpine

WORKDIR /app

# 设置国内 npm 镜像（解决 npx 下载超时）
RUN npm config set registry https://registry.npmmirror.com

COPY package.json .
RUN npm install --omit=dev

# 预装常用 MCP server（避免运行时 npx 下载超时）
RUN npm install -g @modelcontextprotocol/server-memory 2>/dev/null || true

COPY index.mjs .
COPY config.yaml .

USER node

CMD ["node", "index.mjs"]