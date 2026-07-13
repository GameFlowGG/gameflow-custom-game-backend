// Base URL of the GameFlow REST gateway. Defaults to the dev environment; set
// GAMEFLOW_API_URL to point at a local gateway (e.g. http://localhost:5001/v1).
export const GAMEFLOW_API_URL =
  Deno.env.get("GAMEFLOW_API_URL") ?? "https://dev.api.gameflow.gg/v1";

export interface GameServerAllocation {
  address: string;
  port: number;
  serverName: string;
}

export async function startGameServer(payload?: string): Promise<GameServerAllocation> {
  const gameId = Deno.env.get("GAMEFLOW_GAME_ID");
  const apiKey = Deno.env.get("GAMEFLOW_API_KEY");

  if (!gameId || !apiKey) {
    throw new Error("GAMEFLOW_GAME_ID and GAMEFLOW_API_KEY must be set");
  }

  const body: Record<string, unknown> = { timeoutSeconds: 0, region: "us-east" };
  if (payload) {
    body.payload = payload;
  }

  const response = await fetch(
    `${GAMEFLOW_API_URL}/games/${encodeURIComponent(gameId)}/servers`,
    {
      method: "POST",
      headers: {
        "X-Api-Key": apiKey,
      },
      body: JSON.stringify(body),
    }
  );

  if (!response.ok) {
    const body = await response.text();
    throw new Error(`GameFlow server request failed (${response.status}): ${body}`);
  }

  const data = await response.json();
  const server = data.server;

  if (!server?.address || !server?.port) {
    throw new Error("GameFlow returned invalid server data");
  }

  return {
    address: server.address,
    port: server.port,
    serverName: server.name,
  };
}
