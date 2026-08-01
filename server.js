const http = require("http");
const WebSocket = require("ws");

const port = process.env.PORT || 8080;

const httpServer = http.createServer((request, response) => {
    if (request.url === "/health" || request.url === "/") {
        response.writeHead(200, { "Content-Type": "application/json" });
        response.end(JSON.stringify({ status: "ok", service: "signaling" }));
        return;
    }

    response.writeHead(404);
    response.end("Not Found");
});

const wss = new WebSocket.Server({ server: httpServer });

const clients = new Map();

function isValidId(id) {
    return typeof id === "string" && /^[A-Za-z0-9_-]{1,128}$/.test(id);
}

function usersList() {
    return [...clients.values()].map((client) => ({
        id: client.userId,
        name: client.name || "مستخدم",
        avatar_path: client.avatar_path || ""
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

wss.on("connection", (ws) => {

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

                if (clients.has(data.id) && clients.get(data.id) !== ws) {
                    sendError(ws, "User is already connected");
                    ws.close(1008, "User is already connected");
                    return;
                }

                clients.set(data.id, ws);

                ws.userId = data.id;
                ws.name = typeof data.name === "string" ? data.name.slice(0, 80) : "مستخدم";
                ws.avatar_path = typeof data.avatar_path === "string"
                    ? data.avatar_path.slice(0, 500)
                    : "";

                console.log(`${data.id} connected`);
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

            console.log(`${ws.userId} disconnected`);
            sendUsers();

        }

    });

});

httpServer.listen(port, "0.0.0.0", () => {
    console.log(`Server started on port ${port}`);
});