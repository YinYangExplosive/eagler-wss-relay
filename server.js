
const http = require("http");
const WebSocket = require("ws");

const PORT = process.env.PORT || 10000;
const TARGET = process.env.TARGET_WS;

const server = http.createServer((req, res) => {
  if (req.url === "/healthz") {
    res.writeHead(200, { "Content-Type": "text/plain" });
    return res.end("ok");
  }

  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end("Eagler WSS relay is running");
});

const wss = new WebSocket.Server({ noServer: true });

server.on("upgrade", (req, socket, head) => {
  wss.handleUpgrade(req, socket, head, (client) => {
    wss.emit("connection", client, req);
  });
});

wss.on("connection", (client, req) => {
  if (!TARGET) {
    client.close(1011, "TARGET_WS is not configured");
    return;
  }

  let target;

  try {
    target = new URL(TARGET);
  } catch {
    client.close(1011, "Invalid TARGET_WS");
    return;
  }

  if (!["ws:", "wss:"].includes(target.protocol)) {
    client.close(1011, "TARGET_WS must use ws:// or wss://");
    return;
  }

  const upstream = new WebSocket(target.toString());

  let opened = false;
  const pending = [];

  client.on("message", (data, binary) => {
    if (upstream.readyState === WebSocket.OPEN) {
      upstream.send(data, { binary });
    } else if (!opened && pending.length < 100) {
      pending.push({ data, binary });
    } else if (!opened) {
      client.close(1013, "Upstream is taking too long");
    }
  });

  upstream.on("open", () => {
    opened = true;

    for (const message of pending) {
      if (client.readyState !== WebSocket.OPEN) break;
      upstream.send(message.data, { binary: message.binary });
    }

    pending.length = 0;
  });

  upstream.on("message", (data, binary) => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(data, { binary });
    }
  });

  const closeBoth = () => {
    if (client.readyState < WebSocket.CLOSING) client.close();
    if (upstream.readyState < WebSocket.CLOSING) upstream.close();
  };

  client.on("close", closeBoth);
  upstream.on("close", closeBoth);
  client.on("error", closeBoth);
  upstream.on("error", closeBoth);
});

server.listen(PORT, "0.0.0.0", () => {
  console.log(`Relay listening on port ${PORT}`);
});
