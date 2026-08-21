/**
 * Minimal Bun client for the vNext game-session protocol fixture.
 *
 * Usage: bun run fixtures/bun/game-session-client.ts <port> [token] [room]
 */
import { Socket } from "node:net";

type ClientFrame =
  | { kind: "hello"; token?: string; room: string }
  | { kind: "message"; action: string }
  | { kind: "close_send" }
  | { kind: "cancel" };

type ServerFrame =
  | { kind: "ready"; room: string }
  | { kind: "message"; action: string }
  | { kind: "peer_half_closed" }
  | { kind: "terminal"; outcome: unknown }
  | { kind: "rejected"; code: string }
  | { kind: "runtime"; code: string };

const port = Number(process.argv[2] ?? "0");
const token = process.argv[3] ?? "good-token";
const room = process.argv[4] ?? "arena";

if (!Number.isInteger(port) || port <= 0) {
  throw new Error("usage: bun run fixtures/bun/game-session-client.ts <port> [token] [room]");
}

const socket = new Socket();
let pending = Buffer.alloc(0);
let sentAction = false;
let sentClose = false;

function send(frame: ClientFrame): void {
  const payload = Buffer.from(JSON.stringify(frame), "utf8");
  const header = Buffer.allocUnsafe(4);
  header.writeUInt32BE(payload.length, 0);
  socket.write(Buffer.concat([header, payload]));
}

function consume(): void {
  while (pending.length >= 4) {
    const length = pending.readUInt32BE(0);
    if (pending.length < length + 4) return;
    const payload = pending.subarray(4, length + 4);
    pending = pending.subarray(length + 4);
    const frame = JSON.parse(payload.toString("utf8")) as ServerFrame;
    console.log(JSON.stringify(frame));
    if (frame.kind === "ready" && !sentAction) {
      sentAction = true;
      send({ kind: "message", action: "move:left" });
    } else if (frame.kind === "message" && frame.action.startsWith("ack:") && !sentClose) {
      sentClose = true;
      send({ kind: "close_send" });
    } else if (frame.kind === "terminal" || frame.kind === "rejected" || frame.kind === "runtime") {
      socket.end();
      return;
    }
  }
}

socket.on("data", (chunk) => {
  pending = Buffer.concat([pending, chunk]);
  consume();
});
socket.on("error", (error) => {
  console.error(error);
  process.exitCode = 1;
});
socket.on("close", () => process.exit());
socket.connect(port, "127.0.0.1", () => {
  send({ kind: "hello", token, room });
});
