
const http = require("http");
const WebSocket = require("ws");

const PORT = process.env.PORT || 10000;
const STALE_MS = 45000;
const vehicles = new Map();

const server = http.createServer((req, res) => {
  if (req.url === "/" || req.url === "/health") {
    res.writeHead(200, {
      "Content-Type": "application/json"
    });

    res.end(JSON.stringify({
      ok: true,
      service: "GPS Industrial",
      vehicles: vehicles.size,
      storage: "memory-only"
    }));
  } else {
    res.writeHead(404);
    res.end("Not found");
  }
});

const wss = new WebSocket.Server({
  server,
  path: "/ws"
});

function broadcast(data) {
  const message = JSON.stringify(data);

  for (const client of wss.clients) {
    if (client.readyState === WebSocket.OPEN) {
      client.send(message);
    }
  }
}

function publish() {
  broadcast({
    type: "vehicles",
    vehicles: Array.from(vehicles.values())
  });
}

wss.on("connection", (ws) => {
  ws.isAlive = true;
  ws.vehicleId = null;

  ws.send(JSON.stringify({
    type: "vehicles",
    vehicles: Array.from(vehicles.values())
  }));

  ws.on("pong", () => {
    ws.isAlive = true;
  });

  ws.on("message", (message) => {
    let data;

    try {
      data = JSON.parse(message.toString());
    } catch {
      return;
    }

    if (data.type === "ping") {
      ws.send(JSON.stringify({ type: "pong" }));
      return;
    }

    if (data.type === "position") {
      const id = String(data.vehicleId || "").trim().slice(0, 30);
      const operator = String(data.operator || "").trim().slice(0, 50);

      const lat = Number(data.lat);
      const lng = Number(data.lng);
      const accuracy = Number(data.accuracy);

      if (!id ||
          !Number.isFinite(lat) ||
          !Number.isFinite(lng) ||
          lat < -90 || lat > 90 ||
          lng < -180 || lng > 180 ||
          !Number.isFinite(accuracy)) {
        return;
      }

      if (ws.vehicleId && ws.vehicleId !== id) {
        const previous = vehicles.get(ws.vehicleId);

        if (previous && previous.connectionId === ws) {
          vehicles.delete(ws.vehicleId);
        }
      }

      // Evita que dos celulares publiquen el mismo vehículo.
      const existing = vehicles.get(id);

      if (existing && existing.connectionId !== ws) {
        ws.send(JSON.stringify({
          type: "error",
          message: "Este autoelevador ya está siendo compartido."
        }));
        return;
      }

      ws.vehicleId = id;

      vehicles.set(id, {
        vehicleId: id,
        operator: operator || "No indicado",
        lat,
        lng,
        accuracy: Math.round(accuracy),
        speed: data.speed === null ? null : Number(data.speed),
        gpsTime: Number(data.gpsTime) || Date.now(),
        receivedAt: Date.now(),
        connectionId: ws
      });

      publish();
      return;
    }

    if (data.type === "stop") {
      const id = String(data.vehicleId || "").trim();
      const current = vehicles.get(id);

      if (current && current.connectionId === ws) {
        vehicles.delete(id);
        publish();
      }

      ws.vehicleId = null;
    }
  });

  ws.on("close", () => {
    if (ws.vehicleId) {
      const current = vehicles.get(ws.vehicleId);

      if (current && current.connectionId === ws) {
        vehicles.delete(ws.vehicleId);
        publish();
      }
    }
  });
});

// Limpieza automática de posiciones antiguas.
setInterval(() => {
  let changed = false;
  const now = Date.now();

  for (const [id, vehicle] of vehicles.entries()) {
    if (now - vehicle.receivedAt > STALE_MS) {
      vehicles.delete(id);
      changed = true;
    }
  }

  if (changed) publish();

  for (const ws of wss.clients) {
    if (!ws.isAlive) {
      ws.terminate();
    } else {
      ws.isAlive = false;
      ws.ping();
    }
  }
}, 15000);

server.listen(PORT, "0.0.0.0", () => {
  console.log("GPS Industrial funcionando en puerto " + PORT);
});

