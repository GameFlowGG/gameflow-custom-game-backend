import { requireAuth } from "../utils/auth.ts";
import { getAllPublicLobbies } from "../utils/lobby.ts";

export async function handleGetLobbies(request: Request): Promise<Response> {
  console.log("GET /lobbies - Request received");
  
  try {
    await requireAuth(request.headers);

    const lobbies = await getAllPublicLobbies();
    
    console.log("Returning", lobbies.length, "lobbies to client");

    return Response.json(lobbies);
  } catch (error) {
    console.log("GET /lobbies failed:", (error as Error).message);
    return Response.json(
      { error: (error as Error).message || "Unauthorized" },
      { status: 401 }
    );
  }
}
