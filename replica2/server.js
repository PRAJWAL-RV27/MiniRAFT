const express = require("express");
const axios = require("axios");

const app = express();
app.use(express.json({ limit: "2mb" }));

const NODE_ID = process.env.NODE_ID || "replica1";
const PORT = Number(process.env.PORT || 9001);
const PEERS = (process.env.PEERS || "")
  .split(",")
  .map((peer) => peer.trim())
  .filter(Boolean);
const GATEWAY_URL = process.env.GATEWAY_URL || "http://gateway:8080";

const HEARTBEAT_INTERVAL_MS = 150;
const ELECTION_TIMEOUT_MIN_MS = 500;
const ELECTION_TIMEOUT_MAX_MS = 800;
const RPC_TIMEOUT_MS = 700;

let currentTerm = 0;
let votedFor = null;
let state = "follower";
let leaderId = null;

let log = [];
let commitIndex = -1;
let heartbeatTimer = null;
let electionTimer = null;
let syncInProgress = false;
let nextIndexByPeer = new Map();

function randomElectionTimeout() {
  return (
    ELECTION_TIMEOUT_MIN_MS +
    Math.floor(Math.random() * (ELECTION_TIMEOUT_MAX_MS - ELECTION_TIMEOUT_MIN_MS + 1))
  );
}

function majorityCount() {
  const clusterSize = PEERS.length + 1;
  return Math.floor(clusterSize / 2) + 1;
}

function isLeader() {
  return state === "leader";
}

function clampLeaderNextIndex() {
  for (const [peerUrl, nextIndex] of nextIndexByPeer.entries()) {
    nextIndexByPeer.set(peerUrl, Math.max(0, Math.min(nextIndex, log.length)));
  }
}

function initializeLeaderNextIndex() {
  nextIndexByPeer.clear();
  const nextIndex = log.length;
  for (const peer of PEERS) {
    nextIndexByPeer.set(peer, nextIndex);
  }
}

function lastLogIndex() {
  return log.length - 1;
}

function lastLogTerm() {
  if (log.length === 0) {
    return 0;
  }
  return log[log.length - 1].term;
}

function committedEntries() {
  if (commitIndex < 0) {
    return [];
  }
  return log.slice(0, commitIndex + 1);
}

function clearHeartbeatTimer() {
  if (heartbeatTimer) {
    clearInterval(heartbeatTimer);
    heartbeatTimer = null;
  }
}

function clearElectionTimer() {
  if (electionTimer) {
    clearTimeout(electionTimer);
    electionTimer = null;
  }
}

function logInfo(message, payload = {}) {
  const data = {
    nodeId: NODE_ID,
    state,
    term: currentTerm,
    commitIndex,
    logLength: log.length,
    ...payload,
  };
  console.log(`[${NODE_ID}] ${message}`, data);
}

function becomeFollower(newTerm, newLeaderId = null) {
  if (newTerm > currentTerm) {
    currentTerm = newTerm;
    votedFor = null;
  }
  state = "follower";
  nextIndexByPeer.clear();
  if (newLeaderId) {
    leaderId = newLeaderId;
  }
  clearHeartbeatTimer();
  resetElectionTimer();
}

function startHeartbeatLoop() {
  clearHeartbeatTimer();
  heartbeatTimer = setInterval(async () => {
    if (!isLeader()) {
      return;
    }
    await Promise.all(PEERS.map((peer) => sendHeartbeat(peer)));
  }, HEARTBEAT_INTERVAL_MS);
}

function resetElectionTimer() {
  clearElectionTimer();
  electionTimer = setTimeout(() => {
    if (!isLeader()) {
      startElection();
    }
  }, randomElectionTimeout());
}

function isCandidateLogUpToDate(candidateLastTerm, candidateLastIndex) {
  const myLastTerm = lastLogTerm();
  const myLastIndex = lastLogIndex();
  if (candidateLastTerm > myLastTerm) {
    return true;
  }
  if (candidateLastTerm < myLastTerm) {
    return false;
  }
  return candidateLastIndex >= myLastIndex;
}

async function postWithTimeout(url, path, payload, timeout = RPC_TIMEOUT_MS) {
  const fullUrl = `${url}${path}`;
  return axios.post(fullUrl, payload, { timeout });
}

function leaderPeerUrl() {
  if (!leaderId) {
    return null;
  }
  return PEERS.find((peer) => peer.includes(leaderId)) || null;
}

async function requestSyncFromLeader(startIndex) {
  if (syncInProgress || isLeader() || !leaderId) {
    return;
  }

  const targetLeaderUrl = leaderPeerUrl();
  if (!targetLeaderUrl) {
    return;
  }

  syncInProgress = true;
  try {
    const response = await postWithTimeout(
      targetLeaderUrl,
      "/sync-log",
      {
        term: currentTerm,
        requesterId: NODE_ID,
        startIndex: Math.max(0, Number(startIndex) || 0),
      },
      600
    );

    const data = response.data || {};

    if (data.term > currentTerm) {
      becomeFollower(data.term, leaderId);
      return;
    }

    if (!data.success || !Array.isArray(data.entries)) {
      return;
    }

    let safeStart = Math.max(0, Number(startIndex) || 0);
    if (safeStart <= commitIndex) {
      safeStart = commitIndex + 1;
    }

    if (safeStart < log.length) {
      log = log.slice(0, safeStart);
    }

    for (const entry of data.entries) {
      log.push(entry);
    }

    applyLeaderCommit(
      Number.isInteger(data.leaderCommit) ? data.leaderCommit : commitIndex
    );

    logInfo("Synced missing entries from leader", {
      requestedFrom: startIndex,
      received: data.entries.length,
      leader: targetLeaderUrl,
    });
  } catch (error) {
    logInfo("Follower sync request failed", { error: error.message });
  } finally {
    syncInProgress = false;
  }
}

async function sendHeartbeat(peerUrl) {
  try {
    const response = await postWithTimeout(peerUrl, "/heartbeat", {
      term: currentTerm,
      leaderId: NODE_ID,
      leaderCommit: commitIndex,
    });

    if (response.data.term > currentTerm) {
      logInfo("Stepping down due to higher term heartbeat response", {
        peerUrl,
        peerTerm: response.data.term,
      });
      becomeFollower(response.data.term);
    }
  } catch (error) {
    logInfo("Heartbeat peer unavailable", { peerUrl, error: error.message });
  }
}

async function requestVoteFromPeer(peerUrl, votePayload) {
  try {
    const response = await postWithTimeout(peerUrl, "/request-vote", votePayload);
    return response.data;
  } catch (error) {
    return {
      term: currentTerm,
      voteGranted: false,
      error: error.message,
    };
  }
}

async function startElection() {
  state = "candidate";
  currentTerm += 1;
  votedFor = NODE_ID;
  leaderId = null;

  const electionTerm = currentTerm;
  let votes = 1;

  logInfo("Starting election", { electionTerm });
  resetElectionTimer();

  const votePayload = {
    term: electionTerm,
    candidateId: NODE_ID,
    lastLogIndex: lastLogIndex(),
    lastLogTerm: lastLogTerm(),
  };

  const results = await Promise.all(PEERS.map((peer) => requestVoteFromPeer(peer, votePayload)));

  if (state !== "candidate" || currentTerm !== electionTerm) {
    return;
  }

  for (const result of results) {
    if (result.term > currentTerm) {
      logInfo("Found higher term during election", { higherTerm: result.term });
      becomeFollower(result.term);
      return;
    }

    if (result.voteGranted) {
      votes += 1;
    }
  }

  if (votes >= majorityCount()) {
    state = "leader";
    leaderId = NODE_ID;

    // Discard any stale uncommitted suffix so replication restarts from committed state.
    if (commitIndex < log.length - 1) {
      log = log.slice(0, commitIndex + 1);
      logInfo("Trimmed uncommitted suffix on leadership", { logLength: log.length });
      clampLeaderNextIndex();
    }

    initializeLeaderNextIndex();

    clearElectionTimer();
    logInfo("Became leader", { votes, needed: majorityCount() });
    startHeartbeatLoop();
  } else {
    logInfo("Election split vote, will retry", { votes, needed: majorityCount() });
    resetElectionTimer();
  }
}

function applyLeaderCommit(leaderCommit) {
  const newCommitIndex = Math.min(leaderCommit, lastLogIndex());
  if (newCommitIndex > commitIndex) {
    commitIndex = newCommitIndex;
    logInfo("Commit index advanced", { commitIndex });
  }
}

function appendEntriesAt(prevLogIndex, entries) {
  for (let offset = 0; offset < entries.length; offset += 1) {
    const index = prevLogIndex + 1 + offset;
    const incoming = entries[offset];
    const existing = log[index];

    if (!existing) {
      log[index] = incoming;
      continue;
    }

    if (existing.term === incoming.term && existing.id === incoming.id) {
      continue;
    }

    if (index <= commitIndex) {
      return {
        ok: false,
        conflictIndex: commitIndex + 1,
        reason: "would-overwrite-committed-entry",
      };
    }

    log = log.slice(0, index);
    log[index] = incoming;
  }

  return { ok: true };
}

app.post("/request-vote", (req, res) => {
  const { term, candidateId, lastLogIndex: candidateLastIndex, lastLogTerm: candidateLastTerm } = req.body;

  if (term < currentTerm) {
    return res.json({ term: currentTerm, voteGranted: false });
  }

  if (term > currentTerm) {
    becomeFollower(term);
  }

  const hasValidLog = isCandidateLogUpToDate(candidateLastTerm, candidateLastIndex);
  const canVote = votedFor === null || votedFor === candidateId;

  if (canVote && hasValidLog) {
    votedFor = candidateId;
    leaderId = null;
    resetElectionTimer();
    logInfo("Granted vote", { candidateId, term });
    return res.json({ term: currentTerm, voteGranted: true });
  }

  return res.json({ term: currentTerm, voteGranted: false });
});

app.post("/heartbeat", (req, res) => {
  const { term, leaderId: incomingLeaderId, leaderCommit = -1 } = req.body;

  if (term < currentTerm) {
    return res.json({ term: currentTerm, success: false });
  }

  becomeFollower(term, incomingLeaderId);
  applyLeaderCommit(leaderCommit);

  if (leaderCommit > lastLogIndex()) {
    requestSyncFromLeader(lastLogIndex() + 1).catch(() => null);
  }

  return res.json({ term: currentTerm, success: true });
});

app.post("/append-entries", (req, res) => {
  const {
    term,
    leaderId: incomingLeaderId,
    prevLogIndex,
    prevLogTerm,
    entries = [],
    leaderCommit = -1,
  } = req.body;

  if (term < currentTerm) {
    return res.json({ term: currentTerm, success: false, conflictIndex: log.length });
  }

  becomeFollower(term, incomingLeaderId);

  if (prevLogIndex >= 0) {
    if (prevLogIndex >= log.length) {
      requestSyncFromLeader(log.length).catch(() => null);
      return res.json({ term: currentTerm, success: false, conflictIndex: log.length });
    }

    if (log[prevLogIndex].term !== prevLogTerm) {
      requestSyncFromLeader(prevLogIndex).catch(() => null);
      return res.json({ term: currentTerm, success: false, conflictIndex: prevLogIndex });
    }
  }

  const appendResult = appendEntriesAt(prevLogIndex, entries);
  if (!appendResult.ok) {
    return res.json({
      term: currentTerm,
      success: false,
      conflictIndex: appendResult.conflictIndex,
      reason: appendResult.reason,
    });
  }

  applyLeaderCommit(leaderCommit);

  return res.json({ term: currentTerm, success: true, matchIndex: lastLogIndex() });
});

app.post("/sync-log", (req, res) => {
  const {
    term,
    leaderId: incomingLeaderId,
    startIndex = 0,
    entries,
    leaderCommit = -1,
  } = req.body;

  if (term < currentTerm) {
    return res.json({ term: currentTerm, success: false });
  }

  if (!Array.isArray(entries)) {
    if (term > currentTerm) {
      becomeFollower(term, incomingLeaderId || null);
    }

    if (!isLeader()) {
      return res.status(409).json({
        term: currentTerm,
        success: false,
        message: "not-leader",
        leaderId,
      });
    }

    const safeStart = Math.max(0, Number(startIndex) || 0);
    const entriesFromStart = committedEntries().slice(safeStart);
    return res.json({
      term: currentTerm,
      success: true,
      startIndex: safeStart,
      entries: entriesFromStart,
      leaderCommit: commitIndex,
    });
  }

  becomeFollower(term, incomingLeaderId);

  let safeStart = Math.max(0, Number(startIndex));
  if (safeStart <= commitIndex) {
    safeStart = commitIndex + 1;
  }

  if (safeStart < log.length) {
    log = log.slice(0, safeStart);
  }

  for (let i = 0; i < entries.length; i += 1) {
    log.push(entries[i]);
  }

  applyLeaderCommit(leaderCommit);

  return res.json({ term: currentTerm, success: true, logLength: log.length, commitIndex });
});

async function notifyGateway(entry, index) {
  try {
    await postWithTimeout(
      GATEWAY_URL,
      "/replica-commit",
      {
        nodeId: NODE_ID,
        leaderId: NODE_ID,
        term: currentTerm,
        index,
        entry,
      },
      RPC_TIMEOUT_MS
    );
  } catch (error) {
    logInfo("Gateway notify failed", { error: error.message });
  }
}

async function replicateToFollower(peerUrl, entryIndex) {
  if (!isLeader()) {
    return false;
  }

  let nextIndex = nextIndexByPeer.get(peerUrl);
  if (!Number.isInteger(nextIndex)) {
    nextIndex = entryIndex;
  }

  const maxAttempts = 6;
  for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
    if (!isLeader()) {
      return false;
    }

    if (nextIndex > entryIndex) {
      nextIndexByPeer.set(peerUrl, nextIndex);
      return true;
    }

    const prevLogIndex = nextIndex - 1;
    const prevLogTerm = prevLogIndex >= 0 ? log[prevLogIndex]?.term || 0 : 0;
    const entries = log.slice(nextIndex, entryIndex + 1);

    if (entries.length === 0) {
      nextIndexByPeer.set(peerUrl, nextIndex);
      return true;
    }

    try {
      const response = await postWithTimeout(
        peerUrl,
        "/append-entries",
        {
          term: currentTerm,
          leaderId: NODE_ID,
          prevLogIndex,
          prevLogTerm,
          entries,
          leaderCommit: commitIndex,
        },
        RPC_TIMEOUT_MS
      );

      const data = response.data || {};
      if (data.term > currentTerm) {
        becomeFollower(data.term);
        return false;
      }

      if (data.success) {
        nextIndex = entryIndex + 1;
        nextIndexByPeer.set(peerUrl, nextIndex);
        return true;
      }

      if (data.reason === "would-overwrite-committed-entry") {
        logInfo("Stepping down due to committed conflict with follower", {
          peerUrl,
          conflictIndex: data.conflictIndex,
        });
        becomeFollower(currentTerm + 1);
        return false;
      }

      if (Number.isInteger(data.conflictIndex)) {
        nextIndex = Math.max(0, Math.min(data.conflictIndex, entryIndex));
      } else {
        nextIndex = Math.max(0, nextIndex - 1);
      }

      nextIndexByPeer.set(peerUrl, nextIndex);
    } catch (error) {
      if (attempt === maxAttempts - 1) {
        logInfo("Replication retry exhausted", { peerUrl, error: error.message });
      }
    }
  }

  return false;
}

async function commitClientCommand(req, res) {
  if (!isLeader()) {
    return res.status(409).json({
      ok: false,
      message: "not-leader",
      leaderId,
      term: currentTerm,
    });
  }

  const action = req.body.action || req.body.type;
  if (action !== "stroke" && action !== "clear") {
    return res.status(400).json({ ok: false, message: "invalid-action" });
  }

  if (action === "stroke" && !req.body.stroke) {
    return res.status(400).json({ ok: false, message: "missing-stroke" });
  }

  const entry = {
    id: `${NODE_ID}-${Date.now()}-${Math.floor(Math.random() * 1e6)}`,
    action,
    term: currentTerm,
    stroke: req.body.stroke || null,
    timestamp: Date.now(),
  };

  log.push(entry);
  const entryIndex = log.length - 1;

  const replicationResults = await Promise.all(PEERS.map((peer) => replicateToFollower(peer, entryIndex)));

  let ackCount = 1;
  for (const ok of replicationResults) {
    if (ok) {
      ackCount += 1;
    }
  }

  if (ackCount >= majorityCount()) {
    commitIndex = entryIndex;
    await Promise.all([
      notifyGateway(entry, entryIndex),
      ...PEERS.map((peer) =>
        postWithTimeout(peer, "/heartbeat", {
          term: currentTerm,
          leaderId: NODE_ID,
          leaderCommit: commitIndex,
        }).catch(() => null)
      ),
    ]);

    logInfo("Committed entry", { entryIndex, ackCount, action });
    return res.json({ ok: true, committed: true, index: entryIndex, term: currentTerm, entry });
  }

  // Keep leader/follower logs convergent: remove speculative entry when it cannot be committed.
  log.pop();
  clampLeaderNextIndex();

  logInfo("Failed to commit due to missing quorum", { entryIndex, ackCount, action });
  return res.status(503).json({
    ok: false,
    committed: false,
    message: "quorum-unavailable",
    ackCount,
    needed: majorityCount(),
  });
}

app.post("/client-command", commitClientCommand);
app.post("/client-stroke", commitClientCommand);

app.get("/log", (req, res) => {
  return res.json({
    nodeId: NODE_ID,
    term: currentTerm,
    commitIndex,
    entries: committedEntries(),
  });
});

app.get("/status", (req, res) => {
  return res.json({
    nodeId: NODE_ID,
    state,
    term: currentTerm,
    votedFor,
    leaderId,
    logLength: log.length,
    commitIndex,
    healthy: true,
  });
});

app.get("/health", (req, res) => {
  return res.json({ ok: true, nodeId: NODE_ID, state, term: currentTerm });
});

const server = app.listen(PORT, () => {
  logInfo("Replica started", { port: PORT, peers: PEERS, gateway: GATEWAY_URL });
  resetElectionTimer();
});

function shutdown(signal) {
  logInfo("Shutting down replica", { signal });
  clearHeartbeatTimer();
  clearElectionTimer();
  server.close(() => {
    process.exit(0);
  });

  setTimeout(() => {
    process.exit(0);
  }, 2000).unref();
}

process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));
