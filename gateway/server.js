const express = require("express");
const http = require("http");
const path = require("path");
const axios = require("axios");
const WebSocket = require("ws");

const app = express();
app.use(express.json({ limit: "2mb" }));

const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

const PORT = Number(process.env.PORT || 8080);
const REPLICA_URLS = (process.env.REPLICA_URLS || "http://replica1:9001,http://replica2:9002,http://replica3:9003")
    .split(",")
    .map((url) => url.trim())
    .filter(Boolean);

let knownLeaderUrl = null;
let knownLeaderId = null;

let clients = new Set();
let inboundStrokeCount = 0;
let committedStrokeCount = 0;

const committedEntryIds = new Set();
let committedEntries = [];

function logInfo(message, payload = {}) {
    console.log(`[gateway] ${message}`, payload);
}

function rememberCommittedEntry(entry) {
    if (!entry || !entry.id || committedEntryIds.has(entry.id)) {
        return false;
    }

    committedEntryIds.add(entry.id);
    committedEntries.push(entry);

    if (committedEntries.length > 10000) {
        const removed = committedEntries.splice(0, committedEntries.length - 10000);
        for (const item of removed) {
            committedEntryIds.delete(item.id);
        }
    }

    return true;
}

function broadcast(data) {
    const msg = JSON.stringify(data);
    for (const client of clients) {
        if (client.readyState === WebSocket.OPEN) {
            client.send(msg);
        }
    }
}

async function fetchReplicaStatus(url) {
    try {
        const response = await axios.get(`${url}/status`, { timeout: 350 });
        return { url, ok: true, ...response.data };
    } catch (error) {
        return {
            url,
            ok: false,
            healthy: false,
            error: error.message,
        };
    }
}

async function discoverLeader() {
    const statuses = await Promise.all(REPLICA_URLS.map((url) => fetchReplicaStatus(url)));
    const alive = statuses.filter((status) => status.ok);

    const directLeader = alive.find((status) => status.state === "leader");
    if (directLeader) {
        knownLeaderUrl = directLeader.url;
        knownLeaderId = directLeader.nodeId;
        return directLeader.url;
    }

    if (knownLeaderId) {
        const fromHint = alive.find((status) => status.nodeId === knownLeaderId);
        if (fromHint) {
            knownLeaderUrl = fromHint.url;
            return knownLeaderUrl;
        }
    }

    knownLeaderUrl = null;
    return null;
}

async function ensureLeaderUrl() {
    if (!knownLeaderUrl) {
        return discoverLeader();
    }

    try {
        const response = await axios.get(`${knownLeaderUrl}/status`, { timeout: 250 });
        if (response.data.state === "leader") {
            knownLeaderId = response.data.nodeId;
            return knownLeaderUrl;
        }
    } catch (error) {
        logInfo("Known leader is unavailable, rediscovering", { error: error.message });
    }

    knownLeaderUrl = null;
    return discoverLeader();
}

async function forwardCommandToLeader(command) {
    let lastError = "leader-unavailable";

    for (let attempt = 0; attempt < 4; attempt += 1) {
        const leaderUrl = await ensureLeaderUrl();
        if (!leaderUrl) {
            lastError = "leader-unavailable";
            continue;
        }

        try {
            const response = await axios.post(
                `${leaderUrl}/client-command`,
                command,
                { timeout: 900 }
            );

            const payload = response.data;
            if (payload && payload.entry) {
                handleCommittedEntry({
                    entry: payload.entry,
                    index: payload.index,
                    term: payload.term,
                    leaderId: knownLeaderId,
                    nodeId: knownLeaderId,
                });
            }

            return { ok: true };
        } catch (error) {
            const statusCode = error.response?.status;
            lastError = error.response?.data?.message || error.message;

            if (statusCode === 409) {
                knownLeaderUrl = null;
                knownLeaderId = null;
                continue;
            }
        }
    }

    return { ok: false, message: lastError };
}

async function fetchCommittedLogFromLeader() {
    const leaderUrl = await ensureLeaderUrl();
    if (!leaderUrl) {
        return { entries: committedEntries, source: "gateway-cache" };
    }

    try {
        const response = await axios.get(`${leaderUrl}/log`, { timeout: 600 });
        if (Array.isArray(response.data.entries)) {
            for (const entry of response.data.entries) {
                rememberCommittedEntry(entry);
            }
            return { entries: response.data.entries, source: leaderUrl };
        }
    } catch (error) {
        logInfo("Failed to fetch leader log, using cache", { error: error.message });
    }

    return { entries: committedEntries, source: "gateway-cache" };
}

function handleCommittedEntry(payload) {
    const { entry, index = -1, term = -1, leaderId = null } = payload;
    if (!entry || !entry.action) {
        return;
    }

    const inserted = rememberCommittedEntry(entry);
    if (!inserted) {
        return;
    }

    if (entry.action === "stroke") {
        committedStrokeCount += 1;
        broadcast({
            type: "stroke",
            stroke: entry.stroke,
            entryId: entry.id,
            index,
            term,
            leaderId,
        });
        return;
    }

    if (entry.action === "clear") {
        broadcast({
            type: "clear",
            entryId: entry.id,
            index,
            term,
            leaderId,
        });
    }
}

wss.on("connection", async (ws) => {
    clients.add(ws);
    logInfo("Client connected", { connectedClients: clients.size });

    const snapshot = await fetchCommittedLogFromLeader();
    ws.send(
        JSON.stringify({
            type: "snapshot",
            entries: snapshot.entries,
            source: snapshot.source,
        })
    );

    ws.on("message", async (rawMessage) => {
        try {
            const data = JSON.parse(rawMessage);
            if (data.type !== "stroke" && data.type !== "clear") {
                return;
            }

            if (data.type === "stroke" && !data.stroke) {
                return;
            }

            inboundStrokeCount += 1;
            const result = await forwardCommandToLeader(data);

            if (!result.ok) {
                ws.send(
                    JSON.stringify({
                        type: "system",
                        level: "warning",
                        message: `Command delayed: ${result.message}`,
                    })
                );
            }
        } catch (error) {
            ws.send(JSON.stringify({ type: "system", level: "error", message: "Invalid message payload" }));
        }
    });

    ws.on("close", () => {
        clients.delete(ws);
        logInfo("Client disconnected", { connectedClients: clients.size });
    });
});

app.post("/replica-commit", (req, res) => {
    const { nodeId, leaderId, entry, index, term } = req.body;

    if (leaderId) {
        knownLeaderId = leaderId;
    }

    if (nodeId && REPLICA_URLS.length > 0) {
        const byNodeId = REPLICA_URLS.find((url) => url.includes(nodeId));
        if (byNodeId) {
            knownLeaderUrl = byNodeId;
        }
    }

    handleCommittedEntry({ nodeId, leaderId, entry, index, term });
    return res.json({ ok: true });
});

app.get("/stats", async (req, res) => {
    const leaderUrl = await ensureLeaderUrl();
    return res.json({
        clients: clients.size,
        inboundStrokes: inboundStrokeCount,
        committedStrokes: committedStrokeCount,
        leaderUrl,
        leaderId: knownLeaderId,
        replicas: REPLICA_URLS,
    });
});

app.get("/cluster-status", async (req, res) => {
    const statuses = await Promise.all(REPLICA_URLS.map((url) => fetchReplicaStatus(url)));
    return res.json({
        leaderUrl: knownLeaderUrl,
        leaderId: knownLeaderId,
        replicas: statuses,
    });
});

const frontendDir = path.join(__dirname, "..", "frontend");
app.use("/frontend", express.static(frontendDir));
app.get("/dashboard", (req, res) => {
    res.sendFile(path.join(__dirname, "dashboard.html"));
});
app.use(express.static(frontendDir));

server.listen(PORT, async () => {
    logInfo("Gateway started", { port: PORT, replicas: REPLICA_URLS });
    await discoverLeader();
});
