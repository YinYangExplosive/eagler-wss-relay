
const http = require("http");
const WebSocket = require("ws");

const PORT = process.env.PORT || 10000;
const TARGET = process.env.TARGET_WS;

const server = http.createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end("Eagler WSS relay is running");
});

const wss = new WebSocket.Server({ server });

wss.on("connection", (client, req) => {
  if (!TARGET) {
    client.close(1011, "TARGET_WS is not configured");
    return;
  }

  const upstream = new WebSocket(TARGET, {
    headers: {
      "User-Agent": "Eagler-WSS-Relay"
    }
  });

  upstream.on("open", () => {
    client.on("message", (data, binary) => {
      if (upstream.readyState === WebSocket.OPEN) {
        upstream.send(data, { binary });
      }
    });

    upstream.on("message", (data, binary) => {
      if (client.readyState === WebSocket.OPEN) {
        client.send(data, { binary });
      }
    });
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
  console.log(`Relay listening on ${PORT}`);
});
