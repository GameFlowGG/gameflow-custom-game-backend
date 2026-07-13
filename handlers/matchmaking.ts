import type { Session } from "../types/session.ts";
import {
  cancelTicket,
  createTicket,
  getTicketStatus,
  isTransient,
} from "../utils/matchmaking.ts";

// Backend-driven matchmaking (Quick Match). The client asks the backend to find
// a match; the backend holds the API key, enqueues a ticket on the GameFlow
// Matchmaking Frontend, and long-polls until the matchmaker assigns a server.
// Keeping this on the backend means the API key never reaches the client.
//
// The matchmaker forms a full 2v2 (4 players) in FIFO order and assigns them all
// to one game server. On assignment the backend emits `match:started` (the same
// shape the lobby flow uses) so the client reuses its existing connect path.

// One in-flight search per peer. The record is shared with the polling loop so
// `matchmaking:cancel` and disconnects can stop it in place.
interface MatchmakingSearch {
  cancelled: boolean;
  playerId: string;
  ticketId?: string;
  assigned?: boolean;
}

const searches = new Map<string, MatchmakingSearch>();

// Long-poll window (seconds) per status request. The status endpoint returns
// "pending" after this if still unmatched, so the loop simply calls again.
const STATUS_POLL_SECONDS = 20;

// Backoff for transient status failures: start here and double per consecutive
// failure up to the cap, resetting on success. Give up after this many
// consecutive failures so an unreachable backend does not loop forever.
const BACKOFF_START_MS = 1000;
const BACKOFF_MAX_MS = 10000;
const MAX_TRANSIENT_FAILURES = 5;

// handleMatchmakingFind runs the Quick Match flow for one player: enqueue a
// ticket, then poll its status until the matchmaker assigns a server or the
// search is cancelled.
export async function handleMatchmakingFind(
  peerId: string,
  socket: WebSocket,
  session: Session
): Promise<void> {
  if (searches.has(peerId)) {
    sendIfOpen(socket, { type: "error", message: "Already searching for a match" });
    return;
  }

  const search: MatchmakingSearch = { cancelled: false, playerId: session.accountId };
  searches.set(peerId, search);

  try {
    let ticketId: string;
    try {
      ticketId = await createTicket({
        accountId: session.accountId,
        username: session.username,
      });
    } catch (error) {
      // A failed enqueue is terminal (bad request, no published matchmaker, auth):
      // there is nothing to retry against, so report and stop.
      console.error("Matchmaking ticket creation failed:", error);
      sendIfOpen(socket, { type: "matchmaking:error", message: "Failed to enter matchmaking" });
      return;
    }
    search.ticketId = ticketId;

    if (search.cancelled) return;
    sendIfOpen(socket, { type: "matchmaking:searching", ticketId });

    await pollUntilAssigned(socket, search, ticketId);
  } finally {
    searches.delete(peerId);
    // If the search ended without an assignment (cancel, disconnect, error),
    // drop the ticket so it does not linger as an orphan in the queue.
    if (!search.assigned) await cancelSearchTicket(search);
  }
}

// pollUntilAssigned loops on the ticket status until it is assigned a server or
// the search is cancelled. Transient failures back off and retry; a terminal
// failure or too many transient ones report an error and stop.
async function pollUntilAssigned(
  socket: WebSocket,
  search: MatchmakingSearch,
  ticketId: string
): Promise<void> {
  let failures = 0;

  while (!search.cancelled) {
    let result;
    try {
      result = await getTicketStatus(ticketId, STATUS_POLL_SECONDS);
      failures = 0;
    } catch (error) {
      if (!isTransient(error)) {
        console.error("Matchmaking status poll failed (terminal):", error);
        sendIfOpen(socket, { type: "matchmaking:error", message: "Matchmaking failed" });
        return;
      }

      failures++;
      if (failures >= MAX_TRANSIENT_FAILURES) {
        console.error(`Matchmaking status poll failed ${failures}x, giving up:`, error);
        sendIfOpen(socket, { type: "matchmaking:error", message: "Matchmaking unavailable" });
        return;
      }

      const delay = Math.min(BACKOFF_START_MS * 2 ** (failures - 1), BACKOFF_MAX_MS);
      console.warn(`Matchmaking status poll failed (transient, attempt ${failures}), retrying in ${delay}ms:`, error);
      await sleep(delay);
      continue;
    }

    if (search.cancelled) return;

    if (result.status === "assigned" && result.connection) {
      const { address, port } = splitConnection(result.connection);
      const matchData = {
        matchId: crypto.randomUUID(),
        // The matchmaker forms the teams and the game server assigns slots as
        // players connect, so there is no client-visible roster here.
        teamA: [],
        teamB: [],
        startedAt: Date.now(),
        server: { address, port, serverName: "" },
      };
      // Assigned: the ticket is consumed by the match, so it must not be cancelled.
      search.assigned = true;
      sendIfOpen(socket, { type: "match:started", matchData });
      return;
    }
    // status === "pending": the request already long-polled, so loop again.
  }
}

// handleMatchmakingCancel stops the search for this peer and drops its ticket
// from the queue so it does not linger as an orphan and get paired with the next
// player. Best-effort: if the ticket was already matched the drop is a no-op and
// the player simply will not be sent a server.
export function handleMatchmakingCancel(peerId: string, socket: WebSocket): void {
  const search = searches.get(peerId);
  if (search) {
    search.cancelled = true;
    // Drop the ticket now rather than waiting for the poll loop to notice.
    void cancelSearchTicket(search);
  }
  socket.send(JSON.stringify({ type: "matchmaking:cancelled" }));
}

// cancelSearchOnDisconnect stops any in-flight search for a disconnecting peer
// and drops its ticket. Called from the socket close handler.
export async function cancelSearchOnDisconnect(peerId: string): Promise<void> {
  const search = searches.get(peerId);
  if (!search) return;
  search.cancelled = true;
  await cancelSearchTicket(search);
}

// cancelSearchTicket best-effort removes the search's ticket from the queue.
// Clears ticketId so it runs at most once even if called from several paths.
async function cancelSearchTicket(search: MatchmakingSearch): Promise<void> {
  const ticketId = search.ticketId;
  if (!ticketId) return;
  search.ticketId = undefined;
  try {
    await cancelTicket(ticketId, search.playerId);
  } catch (error) {
    console.error("Matchmaking ticket cancel failed:", error);
  }
}

// splitConnection parses a "host:port" connection string. Uses the last colon
// so IPv6 hosts are handled.
function splitConnection(connection: string): { address: string; port: number } {
  const idx = connection.lastIndexOf(":");
  if (idx === -1) {
    return { address: connection, port: 0 };
  }
  return {
    address: connection.slice(0, idx),
    port: Number(connection.slice(idx + 1)),
  };
}

function sendIfOpen(socket: WebSocket, message: unknown): void {
  if (socket.readyState === WebSocket.OPEN) {
    socket.send(JSON.stringify(message));
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
