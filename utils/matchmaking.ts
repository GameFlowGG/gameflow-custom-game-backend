import { GAMEFLOW_API_URL } from "./gameflow.ts";

// Client-facing matchmaking: enqueue a ticket and poll for the assigned server.
// Proxies to the GameFlow Matchmaking Frontend (BFF) at
// POST /v1/matchmaking/tickets and GET /v1/matchmaking/tickets/{id}/status.

export interface MatchmakingPlayer {
  accountId: string;
  username: string;
}

export interface TicketStatus {
  status: string;        // "queued" | "pending" | "assigned"
  connection?: string;   // "host:port", present once status === "assigned"
}

// Pong has no skill-rating integration, so every ticket carries a neutral
// rating. This example uses a plain FIFO matchmaker (players are paired in the
// order they queue), so mu/sigma are placeholders and do not affect matching —
// they are sent only because the API requires sigma > 0.
const DEFAULT_MU = 25;
const DEFAULT_SIGMA = 8.333;

// MatchmakingApiError carries the HTTP status of a failed request (0 for a
// network-level failure) so callers can tell a transient failure (worth
// retrying) from a terminal one (give up). See isTransient.
export class MatchmakingApiError extends Error {
  readonly status: number;

  constructor(status: number, message: string) {
    super(message);
    this.name = "MatchmakingApiError";
    this.status = status;
  }
}

// isTransient reports whether a failed request is worth retrying: network
// errors (status 0), request timeout (408), rate limiting (429), and any 5xx.
// Everything else (4xx: bad ticket, auth) is terminal. Non-API errors are
// treated as transient; a retry cap in the caller stops runaway loops.
export function isTransient(error: unknown): boolean {
  if (!(error instanceof MatchmakingApiError)) return true;
  const { status } = error;
  return status === 0 || status === 408 || status === 429 || status >= 500;
}

function gameflowGame(): { gameId: string; apiKey: string } {
  const gameId = Deno.env.get("GAMEFLOW_GAME_ID");
  const apiKey = Deno.env.get("GAMEFLOW_API_KEY");

  if (!gameId || !apiKey) {
    throw new Error("GAMEFLOW_GAME_ID and GAMEFLOW_API_KEY must be set");
  }

  return { gameId, apiKey };
}

function gameMode(): string {
  return Deno.env.get("GAMEFLOW_GAME_MODE") || "default";
}

function region(): string {
  return Deno.env.get("GAMEFLOW_REGION") || "us-east";
}

// request wraps fetch so a network-level failure surfaces as a
// MatchmakingApiError with status 0 instead of an opaque TypeError.
async function request(url: string, init: RequestInit): Promise<Response> {
  try {
    return await fetch(url, init);
  } catch (error) {
    const detail = error instanceof Error ? error.message : String(error);
    throw new MatchmakingApiError(0, `matchmaking request failed: ${detail}`);
  }
}

// createTicket enqueues a matchmaking ticket for a single player and returns the
// ticket id. Throws a MatchmakingApiError if the (game_id, game_mode) has no
// published matchmaker (FailedPrecondition -> 400) or the request fails.
export async function createTicket(player: MatchmakingPlayer): Promise<string> {
  const { gameId, apiKey } = gameflowGame();

  const response = await request(`${GAMEFLOW_API_URL}/matchmaking/tickets`, {
    method: "POST",
    headers: {
      "X-Api-Key": apiKey,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      player_id: player.accountId,
      game_id: gameId,
      game_mode: gameMode(),
      preferred_az: region(),
      mu: DEFAULT_MU,
      sigma: DEFAULT_SIGMA,
      rating_bucket: 0,
      tags: [],
    }),
  });

  if (!response.ok) {
    throw new MatchmakingApiError(
      response.status,
      `create ticket failed (${response.status}): ${await response.text()}`
    );
  }

  const data = await response.json();
  const ticketId = data.ticketId ?? data.ticket_id;

  if (!ticketId) {
    throw new MatchmakingApiError(response.status, "matchmaking returned no ticket id");
  }

  return ticketId;
}

// getTicketStatus long-polls the ticket's assignment. It returns as soon as the
// ticket is assigned a server, or after timeoutSeconds with status "pending"
// (still searching) — call it again to keep waiting.
export async function getTicketStatus(
  ticketId: string,
  timeoutSeconds = 20
): Promise<TicketStatus> {
  const { apiKey } = gameflowGame();

  const url = `${GAMEFLOW_API_URL}/matchmaking/tickets/${encodeURIComponent(
    ticketId
  )}/status?timeout_seconds=${timeoutSeconds}`;

  const response = await request(url, {
    headers: { "X-Api-Key": apiKey },
  });

  if (!response.ok) {
    throw new MatchmakingApiError(
      response.status,
      `status request failed (${response.status}): ${await response.text()}`
    );
  }

  const data = await response.json();

  return {
    status: data.status,
    connection: data.connection,
  };
}

// cancelTicket removes a ticket from the queue when the player stops searching
// (cancel or disconnect) before matching, so it does not linger and get paired
// with the next player. player must be the ticket owner. A 404 is treated as
// success (the ticket was already matched or removed).
export async function cancelTicket(ticketId: string, playerId: string): Promise<void> {
  const { apiKey } = gameflowGame();

  const url = `${GAMEFLOW_API_URL}/matchmaking/tickets/${encodeURIComponent(
    ticketId
  )}?player_id=${encodeURIComponent(playerId)}`;

  const response = await request(url, {
    method: "DELETE",
    headers: { "X-Api-Key": apiKey },
  });

  if (!response.ok && response.status !== 404) {
    throw new MatchmakingApiError(
      response.status,
      `cancel request failed (${response.status}): ${await response.text()}`
    );
  }
}
