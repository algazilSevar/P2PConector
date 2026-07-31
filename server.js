const WebSocket = require("ws");

const port = process.env.PORT || 8080;

const wss = new WebSocket.Server({ port });

const clients = new Map();

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

                clients.set(data.id, ws);

                ws.userId = data.id;

                console.log(`${data.id} connected`);

                break;

            case "signal":

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

            clients.delete(ws.userId);

            console.log(`${ws.userId} disconnected`);

        }

    });

});

console.log(`Server started on port ${port}`);