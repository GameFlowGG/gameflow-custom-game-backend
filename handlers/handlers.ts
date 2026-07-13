import { kv } from "../db/kv.ts";
import type { Lobby } from "../types/lobby.ts";
import type { Session } from "../types/session.ts";
import {
  generateUniqueCode,
  getLobby,
  saveLobby,
  deleteLobby,
  getSession,
  saveSession,
  deleteSession,
} from "../utils/lobby.ts";
import { startGameServer } from "../utils/gameflow.ts";
import {
  cancelSearchOnDisconnect,
  handleMatchmakingCancel,
  handleMatchmakingFind,
} from "./matchmaking.ts";

const connections = new Map<string, WebSocket>();
const lobbySubscriptions = new Map<string, Set<string>>();

export function registerConnection(peerId: string, socket: WebSocket): void {
  connections.set(peerId, socket);
}

export function unregisterConnection(peerId: string): void {
  connections.delete(peerId);
}

export function getConnection(peerId: string): WebSocket | undefined {
  return connections.get(peerId);
}

export async function handleMessage(
  peerId: string,
  socket: WebSocket,
  data: string
): Promise<void> {
  const session = (await getSession(peerId)) as Session | null;
  if (!session) return;

  try {
    const message = JSON.parse(data);

    switch (message.type) {
      case "ping":
        socket.send(JSON.stringify({ type: "pong" }));
        break;

      case "lobby:create":
        await handleLobbyCreate(peerId, socket, session, message);
        break;

      case "lobby:join":
        await handleLobbyJoin(peerId, socket, session, message);
        break;

      case "lobby:leave":
        await handleLobbyLeave(peerId, socket, session);
        break;

      case "lobby:ready":
        await handleLobbyReady(peerId, socket, session, message);
        break;

      case "lobby:start":
        await handleLobbyStart(peerId, socket, session);
        break;

      case "lobby:fill-bots":
        console.log('Received lobby:fill-bots message from', session.username);
        await handleLobbyFillBots(peerId, socket, session);
        break;

      case "matchmaking:find":
        await handleMatchmakingFind(peerId, socket, session);
        break;

      case "matchmaking:cancel":
        handleMatchmakingCancel(peerId, socket);
        break;

      default:
        socket.send(
          JSON.stringify({ type: "error", message: "Unknown message type" })
        );
    }
  } catch (_) {
    socket.send(
      JSON.stringify({ type: "error", message: "Invalid message format" })
    );
  }
}

export async function handleDisconnect(peerId: string): Promise<void> {
  const session = (await getSession(peerId)) as Session | null;

  // Stop any in-flight matchmaking search for this peer and drop its ticket so
  // it does not linger as an orphan in the queue.
  await cancelSearchOnDisconnect(peerId);

  if (session?.lobbyId) {
    const lobby = await getLobby(session.lobbyId);

    if (lobby) {
      lobby.teamA = lobby.teamA.filter(
        (p) => p.accountId !== session.accountId
      );
      lobby.teamB = lobby.teamB.filter(
        (p) => p.accountId !== session.accountId
      );

      if (lobby.teamA.length === 0 && lobby.teamB.length === 0) {
        await deleteLobby(lobby);
        publishToLobby(lobby.id, { type: "lobby:deleted", lobbyId: lobby.id });
      } else {
        await saveLobby(lobby);
        publishToLobby(lobby.id, { type: "lobby:updated", lobby });
      }
    }

    unsubscribeFromLobby(peerId, session.lobbyId);
  }

  await deleteSession(peerId);
  unregisterConnection(peerId);
}

async function handleLobbyCreate(
  peerId: string,
  socket: WebSocket,
  session: Session,
  data: { isPrivate?: boolean }
): Promise<void> {
  const lobbyId = crypto.randomUUID();
  const code = await generateUniqueCode();

  const lobby: Lobby = {
    id: lobbyId,
    code,
    isPrivate: data.isPrivate || false,
    teamA: [
      {
        accountId: session.accountId,
        username: session.username,
        ready: false,
      },
    ],
    teamB: [],
    ownerId: session.accountId,
  };

  await saveLobby(lobby);
  console.log(
    `🎮 Lobby created - ID: ${lobbyId}, Code: ${code}, isPrivate: ${lobby.isPrivate}, Owner: ${session.username}`
  );

  session.lobbyId = lobbyId;

  await saveSession(peerId, session);

  subscribeToLobby(peerId, lobbyId);

  socket.send(JSON.stringify({ type: "lobby:created", lobby }));
}

async function handleLobbyJoin(
  peerId: string,
  socket: WebSocket,
  session: Session,
  data: { lobbyId?: string; code?: string; team?: string }
): Promise<void> {
  let targetLobbyId = data.lobbyId;

  if (data.code) {
    const result = await kv.get<string>([`code:${data.code}`]);
    targetLobbyId = result.value || undefined;

    if (!targetLobbyId) {
      socket.send(JSON.stringify({ type: "error", message: "Lobby not found" }));
      return;
    }
  }

  if (!targetLobbyId) {
    socket.send(
      JSON.stringify({ type: "error", message: "Lobby ID or code required" })
    );
    return;
  }

  const lobby = await getLobby(targetLobbyId);

  if (!lobby) {
    socket.send(JSON.stringify({ type: "error", message: "Lobby not found" }));
    return;
  }

  if (lobby.isPrivate && !data.code) {
    socket.send(JSON.stringify({ type: "error", message: "Lobby is private" }));
    return;
  }

  const allPlayers = [...lobby.teamA, ...lobby.teamB];

  if (allPlayers.some((p) => p.accountId === session.accountId)) {
    socket.send(
      JSON.stringify({ type: "error", message: "Already in lobby" })
    );
    return;
  }

  const player = {
    accountId: session.accountId,
    username: session.username,
    ready: false,
  };

  if (data.team === "B") {
    lobby.teamB.push(player);
  } else if (data.team === "A") {
    lobby.teamA.push(player);
  } else {
    if (lobby.teamA.length <= lobby.teamB.length) {
      lobby.teamA.push(player);
    } else {
      lobby.teamB.push(player);
    }
  }

  await saveLobby(lobby);

  session.lobbyId = lobby.id;

  await saveSession(peerId, session);

  subscribeToLobby(peerId, lobby.id);

  publishToLobby(lobby.id, { type: "lobby:updated", lobby });
}

async function handleLobbyLeave(
  peerId: string,
  socket: WebSocket,
  session: Session
): Promise<void> {
  const lobbyId = session.lobbyId;

  if (!lobbyId) {
    socket.send(JSON.stringify({ type: "error", message: "Not in a lobby" }));
    return;
  }

  const lobby = await getLobby(lobbyId);

  if (!lobby) {
    socket.send(JSON.stringify({ type: "error", message: "Lobby not found" }));
    return;
  }

  lobby.teamA = lobby.teamA.filter((p) => p.accountId !== session.accountId);
  lobby.teamB = lobby.teamB.filter((p) => p.accountId !== session.accountId);

  if (lobby.teamA.length === 0 && lobby.teamB.length === 0) {
    await deleteLobby(lobby);
    publishToLobby(lobby.id, { type: "lobby:deleted", lobbyId: lobby.id });
  } else {
    await saveLobby(lobby);
    publishToLobby(lobby.id, { type: "lobby:updated", lobby });
  }

  unsubscribeFromLobby(peerId, lobbyId);

  session.lobbyId = undefined;
  await saveSession(peerId, session);

  socket.send(JSON.stringify({ type: "lobby:left" }));
}

async function handleLobbyReady(
  _: string,
  socket: WebSocket,
  session: Session,
  data: { ready?: boolean }
): Promise<void> {
  const lobbyId = session.lobbyId;

  if (!lobbyId) {
    socket.send(JSON.stringify({ type: "error", message: "Not in a lobby" }));
    return;
  }

  const lobby = await getLobby(lobbyId);

  if (!lobby) {
    socket.send(JSON.stringify({ type: "error", message: "Lobby not found" }));
    return;
  }

  const ready = data.ready !== undefined ? data.ready : true;

  lobby.teamA = lobby.teamA.map((p) =>
    p.accountId === session.accountId ? { ...p, ready } : p
  );
  lobby.teamB = lobby.teamB.map((p) =>
    p.accountId === session.accountId ? { ...p, ready } : p
  );

  await saveLobby(lobby);

  publishToLobby(lobby.id, { type: "lobby:updated", lobby });
}

async function handleLobbyStart(
  _: string,
  socket: WebSocket,
  session: Session
): Promise<void> {
  const lobbyId = session.lobbyId;

  if (!lobbyId) {
    socket.send(JSON.stringify({ type: "error", message: "Not in a lobby" }));
    return;
  }

  const lobby = await getLobby(lobbyId);

  if (!lobby) {
    socket.send(JSON.stringify({ type: "error", message: "Lobby not found" }));
    return;
  }

  if (lobby.ownerId !== session.accountId) {
    socket.send(
      JSON.stringify({
        type: "error",
        message: "Only lobby owner can start match",
      })
    );
    return;
  }

  const allPlayers = [...lobby.teamA, ...lobby.teamB];
  const allReady = allPlayers.every((p) => p.ready);

  if (!allReady) {
    socket.send(
      JSON.stringify({ type: "error", message: "Not all players are ready" })
    );
    return;
  }

  const payload = JSON.stringify({
    players: allPlayers.map((p) => p.accountId),
    teamA: lobby.teamA.map((p) => ({ accountId: p.accountId, username: p.username })),
    teamB: lobby.teamB.map((p) => ({ accountId: p.accountId, username: p.username })),
  });

  let server;
  try {
    server = await startGameServer(payload);
  } catch (error) {
    console.error("GameFlow allocation failed:", error);
    socket.send(
      JSON.stringify({ type: "error", message: "Failed to allocate game server" })
    );
    return;
  }

  const matchId = crypto.randomUUID();
  const teamA = lobby.teamA.map((p) => ({
    accountId: p.accountId,
    username: p.username,
  }));
  const teamB = lobby.teamB.map((p) => ({
    accountId: p.accountId,
    username: p.username,
  }));

  const matchData = {
    matchId,
    lobbyId: lobby.id,
    teamA,
    teamB,
    startedAt: Date.now(),
    server: {
      address: server.address,
      port: server.port,
      serverName: server.serverName,
    },
  };

  publishToLobby(lobby.id, { type: "match:started", matchData });

  await deleteLobby(lobby);
}

async function handleLobbyFillBots(
  _peerId: string,
  socket: WebSocket,
  session: Session
): Promise<void> {
  console.log('Fill with bots requested by:', session.username);
  const lobbyId = session.lobbyId;

  if (!lobbyId) {
    console.error('Error: User not in a lobby');
    socket.send(JSON.stringify({ type: "error", message: "Not in a lobby" }));
    return;
  }

  const lobby = await getLobby(lobbyId);

  if (!lobby) {
    console.error('Error: Lobby not found');
    socket.send(JSON.stringify({ type: "error", message: "Lobby not found" }));
    return;
  }

  if (lobby.ownerId !== session.accountId) {
    console.error('Error: User is not lobby owner');
    socket.send(
      JSON.stringify({ type: "error", message: "Only lobby owner can add bots" })
    );
    return;
  }

  console.log(`Adding bots to lobby ${lobby.id}. Current: Team A=${lobby.teamA.length}, Team B=${lobby.teamB.length}`);

  let botNumber = 1;
  while (lobby.teamA.length < 2) {
    lobby.teamA.push({
      accountId: `bot-${crypto.randomUUID()}`,
      username: `Bot ${botNumber++}`,
      ready: true,
    });
  }
  while (lobby.teamB.length < 2) {
    lobby.teamB.push({
      accountId: `bot-${crypto.randomUUID()}`,
      username: `Bot ${botNumber++}`,
      ready: true,
    });
  }

  console.log(`Bots added. New: Team A=${lobby.teamA.length}, Team B=${lobby.teamB.length}`);

  await saveLobby(lobby);

  console.log('Lobby saved, publishing update...');

  publishToLobby(lobby.id, { type: "lobby:updated", lobby });
  
  console.log('Update published');
}

function subscribeToLobby(peerId: string, lobbyId: string): void {
  if (!lobbySubscriptions.has(lobbyId)) {
    lobbySubscriptions.set(lobbyId, new Set());
  }
  lobbySubscriptions.get(lobbyId)!.add(peerId);
}

function unsubscribeFromLobby(peerId: string, lobbyId: string): void {
  const subs = lobbySubscriptions.get(lobbyId);
  if (subs) {
    subs.delete(peerId);
    if (subs.size === 0) {
      lobbySubscriptions.delete(lobbyId);
    }
  }
}

function publishToLobby(lobbyId: string, message: unknown): void {
  const subs = lobbySubscriptions.get(lobbyId);
  if (!subs) return;

  const messageStr = JSON.stringify(message);
  for (const peerId of subs) {
    const socket = connections.get(peerId);
    if (socket && socket.readyState === WebSocket.OPEN) {
      socket.send(messageStr);
    }
  }
}
