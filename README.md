# Distributed Real-Time Drawing Board (Mini-RAFT)

This project implements a fault-tolerant collaborative drawing system with one gateway and three RAFT-lite replicas.

Users can draw normally and also clear the canvas using a replicated `Clear Board` action, so erase behavior stays consistent across all clients.

## Services

- `gateway` (port `8080`)
  - WebSocket endpoint for browsers
  - Routes incoming strokes to current leader replica
  - Receives committed entries from leader and broadcasts to all clients
  - Serves frontend (`/`) and dashboard (`/dashboard`)
- `replica1` (port `9001`)
- `replica2` (port `9002`)
- `replica3` (port `9003`)

Each replica supports:

- Follower / Candidate / Leader modes
- RequestVote, AppendEntries, Heartbeat, SyncLog RPC endpoints
- Majority commit rule
- Term-based safety and stale leader step-down
- Catch-up of restarted nodes via `/sync-log`

## API Endpoints

### Replica RPC

- `POST /request-vote`
- `POST /append-entries`
- `POST /heartbeat`
- `POST /sync-log` (rejoining follower requests committed suffix from leader)
- `POST /client-stroke` (leader only)
- `GET /status`
- `GET /log`
- `GET /health`

### Gateway APIs

- `POST /replica-commit` (called by leader)
- `GET /stats`
- `GET /cluster-status`
- `GET /dashboard`

## Run with Docker

From project root:

```bash
docker compose up --build
```

Then open:

- Drawing board: `http://localhost:8080`
- Dashboard: `http://localhost:8080/dashboard`

## Run Across Multiple Computers (Recommended for Demo)

The simplest and most reliable way to show "multiple computers" is:

- Run the full cluster on one host machine (server laptop/PC)
- Open the board from other laptops/phones on the same Wi-Fi/LAN

### A. Host machine setup

1. Start Docker Desktop on host machine.
2. Run:

```bash
docker compose up --build
```

3. Find host LAN IP:

- Windows PowerShell: `ipconfig`
- Use IPv4 like `192.168.1.23`

4. Allow inbound firewall for port `8080` (if blocked).

### B. Client machines setup

Open in browser:

- `http://<HOST_IP>:8080`
- Dashboard: `http://<HOST_IP>:8080/dashboard`

Example:

- `http://192.168.1.23:8080`

All clients will draw on the same canvas in real time.

### C. Failover demo with multiple computers connected

1. Keep board open on 2-4 different computers.
2. In dashboard, identify leader replica.
3. Kill leader container on host machine:

```bash
docker kill replica1
```

4. Show that:
   - New leader is elected automatically.
   - Existing clients stay connected.
   - Drawing continues with no refresh.

5. Restart killed node:

```bash
docker start replica1
```

6. Show catch-up (`commitIndex` alignment in dashboard).

## True Multi-Machine Cluster (Advanced)

If your faculty explicitly asks replicas on different computers:

- Run `gateway` + one replica per machine
- Replace Docker service names (`replica1`, `replica2`, `replica3`) with real machine IPs
- Set `PEERS`, `GATEWAY_URL`, `REPLICA_URLS` using reachable `http://<ip>:<port>` addresses
- Open firewall ports: `8080`, `9001`, `9002`, `9003`

For most assignment demos, LAN clients + single-host cluster is accepted and far easier to stabilize.

## Hot Reload and Zero-Downtime Behavior

Replica folders are bind-mounted:

- `./replica1:/app`
- `./replica2:/app`
- `./replica3:/app`

Editing replica source triggers nodemon restart in that container. Remaining replicas continue serving; election runs automatically and gateway reroutes to new leader.

## Suggested Demo Script

1. Open two browser tabs at `http://localhost:8080`.
2. Draw in one tab and verify real-time sync in both tabs.
3. Click `Clear Board` in one tab and verify the canvas clears on every connected client.
4. Open `http://localhost:8080/dashboard`.
5. Identify current leader.
6. Kill leader container:
   - `docker kill replica1` (or whichever is leader)
7. Observe automatic new leader election in dashboard.
8. Continue drawing without refreshing tabs.
9. Restart killed replica:
   - `docker start replica1`
10. Verify rejoined node catches up (`commitIndex` aligns).
11. Edit `replica2/server.js` while running and save.
12. Confirm container auto-reloads and cluster remains available.

## Notes

- Election timeout: random `500-800ms`
- Heartbeat interval: `150ms`
- Majority in 3-node cluster: `2`
- Frontend renders committed strokes only; on reconnect it receives full snapshot.
