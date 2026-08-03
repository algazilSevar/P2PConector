const http = require("http");
const WebSocket = require("ws");

const port = process.env.PORT || 8080;

const httpServer = http.createServer((request, response) => {
    // Enable CORS
    response.setHeader("Access-Control-Allow-Origin", "*");
    response.setHeader("Access-Control-Allow-Methods", "GET, OPTIONS");

    if (request.method === "OPTIONS") {
        response.writeHead(204);
        response.end();
        return;
    }

    if (request.url === "/health" || request.url === "/") {
        response.writeHead(200, { "Content-Type": "application/json" });
        response.end(JSON.stringify({
            status: "ok",
            service: "p2p-signaling",
            online_users: clients.size,
            timestamp: new Date().toISOString()
        }));
        return;
    }

    response.writeHead(404, { "Content-Type": "text/plain" });
    response.end("Not Found");
});

// Use noServer mode and handle upgrade events explicitly so we can
// accept WebSocket connections only on a specific path (e.g. /ws).
const wss = new WebSocket.Server({ noServer: true });

const clients = new Map();

function isValidId(id) {
    return typeof id === "string" && /^[A-Za-z0-9_-]{1,128}$/.test(id);
}

function usersList() {
    return [...clients.values()].map((client) => ({
        id: client.userId,
        name: client.name || "مستخدم",
        avatar_url: client.avatar_url || client.avatar_path || ""
    }));
}

function sendUsers() {
    const payload = JSON.stringify({
        type: "users",
        users: usersList()
    });

    for (const client of clients.values()) {
        if (client.readyState === WebSocket.OPEN) {
            client.send(payload);
        }
    }
}

function sendError(ws, message) {
    if (ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ type: "error", message }));
    }
}

httpServer.on('upgrade', (request, socket, head) => {
    if (request.url === '/ws' || request.url === '/') {
        wss.handleUpgrade(request, socket, head, (ws) => {
            wss.emit('connection', ws, request);
        });
    } else {
        socket.write('HTTP/1.1 404 Not Found\r\n\r\n');
        socket.destroy();
    }
});

wss.on("connection", (ws) => {
    ws.isAlive = true;
    ws.on("pong", () => { ws.isAlive = true; });

    ws.on("message", (message) => {
        let data;
        try {
            data = JSON.parse(message);
        } catch {
            return;
        }

        switch (data.type) {
            case "register":
                if (!isValidId(data.id)) {
                    sendError(ws, "Invalid user id");
                    ws.close(1008, "Invalid user id");
                    return;
                }

                // If user reconnected with same ID, close old socket cleanly
                if (clients.has(data.id) && clients.get(data.id) !== ws) {
                    try {
                        const oldSocket = clients.get(data.id);
                        oldSocket.userId = null;
                        oldSocket.close(1000, "Replaced by new connection");
                    } catch (e) {}
                }

                clients.set(data.id, ws);
                ws.userId = data.id;
                ws.name = typeof data.name === "string" ? data.name.slice(0, 80) : "مستخدم";
                ws.avatar_url = typeof data.avatar_url === "string"
                    ? data.avatar_url.slice(0, 500000)
                    : (typeof data.avatar_path === "string" ? data.avatar_path.slice(0, 500000) : "");

                console.log(`[+] User registered: ${data.id} (${ws.name})`);
                sendUsers();
                break;

            case "signal":
                if (!ws.userId || data.from !== ws.userId || !isValidId(data.target)) {
                    sendError(ws, "Invalid signal sender or target");
                    return;
                }

                const target = clients.get(data.target);
                if (target && target.readyState === WebSocket.OPEN) {
                    target.send(JSON.stringify({
                        type: "signal",
                        from: data.from,
                        signal: data.signal
                    }));
                }
                break;
        }
    });

    ws.on("close", () => {
        if (ws.userId) {
            if (clients.get(ws.userId) === ws) {
                clients.delete(ws.userId);
            }
            console.log(`[-] User disconnected: ${ws.userId}`);
            sendUsers();
        }
    });
});

// Ping-Pong heartbeat every 30 seconds to keep cloud connections active
const pingInterval = setInterval(() => {
    wss.clients.forEach((ws) => {
        if (ws.isAlive === false) return ws.terminate();
        ws.isAlive = false;
        ws.ping();
    });
}, 30000);

wss.on("close", () => {
    clearInterval(pingInterval);
});

// Graceful shutdown handling
process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);

function shutdown() {
    console.log("Shutting down signaling server gracefully...");
    clearInterval(pingInterval);
    wss.clients.forEach((ws) => ws.close(1001, "Server shutting down"));
    httpServer.close(() => {
        console.log("Signaling server stopped.");
        process.exit(0);
    });
}

httpServer.listen(port, "0.0.0.0", () => {
    console.log(`🚀 Signaling Server started on port ${port}`);
});