import { connect } from "cloudflare:sockets";

const DEFAULT_REGION = "eu";

function json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "access-control-allow-origin": "*",
      "cache-control": "no-store",
    },
  });
}

function isWebSocket(request) {
  return request.headers.get("Upgrade")?.toLowerCase() === "websocket";
}

function uuidFromBytes(bytes) {
  const hex = Array.from(bytes).map(b => b.toString(16).padStart(2, "0")).join("");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20, 32)}`;
}

async function toUint8(data) {
  if (data instanceof ArrayBuffer) return new Uint8Array(data);
  if (ArrayBuffer.isView(data)) return new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
  if (data && typeof data.arrayBuffer === "function") {
    // Blob (پیش‌فرض در compatibility dateهای جدید)
    return new Uint8Array(await data.arrayBuffer());
  }
  throw new Error("Unsupported websocket data type: " + typeof data);
}

// early data (0-RTT) از هدر Sec-WebSocket-Protocol
function decodeEarlyData(str) {
  if (!str) return null;
  try {
    const b64 = str.replace(/-/g, "+").replace(/_/g, "/");
    const bin = atob(b64);
    return Uint8Array.from(bin, c => c.charCodeAt(0));
  } catch {
    return null;
  }
}

function parseVLESS(bytes) {
  if (bytes.length < 24) throw new Error("VLESS packet too short");

  const version = bytes[0];
  const uuid = uuidFromBytes(bytes.slice(1, 17));
  const addonsLength = bytes[17];
  let index = 18 + addonsLength;

  if (index + 4 > bytes.length) throw new Error("Invalid VLESS header");

  const command = bytes[index];
  if (command !== 1) throw new Error("Only TCP supported (cmd=" + command + ")");

  const port = (bytes[index + 1] << 8) | bytes[index + 2];
  const addressType = bytes[index + 3];
  index += 4;

  let address = "";

  if (addressType === 1) {
    if (index + 4 > bytes.length) throw new Error("Invalid IPv4");
    address = Array.from(bytes.slice(index, index + 4)).join(".");
    index += 4;
  } else if (addressType === 2) {
    const len = bytes[index++];
    if (index + len > bytes.length) throw new Error("Invalid domain");
    address = new TextDecoder().decode(bytes.slice(index, index + len));
    index += len;
  } else if (addressType === 3) {
    if (index + 16 > bytes.length) throw new Error("Invalid IPv6");
    const parts = [];
    for (let i = 0; i < 16; i += 2) {
      parts.push(((bytes[index + i] << 8) | bytes[index + i + 1]).toString(16));
    }
    address = parts.join(":");
    index += 16;
  } else {
    throw new Error("Unknown address type " + addressType);
  }

  return { version, uuid, address, port, payload: bytes.slice(index) };
}

async function handleVLESS(request, env) {
  if (!isWebSocket(request)) {
    return new Response("WebSocket Required", { status: 426 });
  }

  const pair = new WebSocketPair();
  const client = pair[0];
  const server = pair[1];
  server.accept();
  try { server.binaryType = "arraybuffer"; } catch {}

  let remote = null;
  let writer = null;
  let closed = false;

  const closeAll = () => {
    if (closed) return;
    closed = true;
    try { remote?.close(); } catch {}
    try { server.close(); } catch {}
  };

  // اتصال به مقصد + pipe کردن جواب به websocket
  async function dial(host, port, payload, header, allowRetry) {
    const sock = connect({
  hostname: env.RELAY_HOST,
  port: Number(env.RELAY_PORT)
});

remote = sock;

writer = sock.writable.getWriter();


// ساخت هدر برای Relay
const hostBuffer = new TextEncoder().encode(host);

const relayHeader = new Uint8Array(
  2 +
  hostBuffer.length +
  2 +
  payload.length
);


// طول دامنه
relayHeader[0] = (hostBuffer.length >> 8) & 0xff;
relayHeader[1] = hostBuffer.length & 0xff;


// دامنه
relayHeader.set(
  hostBuffer,
  2
);


// پورت مقصد
relayHeader[
  2 + hostBuffer.length
] = (port >> 8) & 0xff;

relayHeader[
  3 + hostBuffer.length
] = port & 0xff;


// دیتاهای اولیه VLESS
if (payload.length) {

  relayHeader.set(
    payload,
    4 + hostBuffer.length
  );

}


await writer.write(
  relayHeader
);

    let gotData = false;
    let hdr = header; // هدر پاسخ VLESS باید همراه اولین chunk برگرده

    sock.readable
      .pipeTo(
        new WritableStream({
          write(chunk) {
            if (closed) return;
            gotData = true;
            if (hdr) {
              const merged = new Uint8Array(hdr.length + chunk.length);
              merged.set(hdr, 0);
              merged.set(chunk, hdr.length);
              server.send(merged);
              hdr = null;
            } else {
              server.send(chunk);
            }
          },
        })
      )
      .catch(() => {})
      .finally(async () => {
        // اگه هیچ دیتایی نیومد (مثلا مقصد پشت کلودفلر بود) با PROXY_IP دوباره تلاش کن
        if (!gotData && allowRetry && env.PROXY_IP && !closed) {
          try {
            const [h, p] = env.PROXY_IP.split(":");
            await dial(h, p ? Number(p) : port, payload, header, false);
          } catch {
            closeAll();
          }
        } else {
          closeAll();
        }
      });
  }

  async function onMessage(data) {
    const bytes = await toUint8(data);

    // پکت‌های بعدی: مستقیم بنویس روی سوکت
    if (writer) {
      await writer.write(bytes);
      return;
    }

    // اولین پکت: هدر VLESS
    const parsed = parseVLESS(bytes);

    if (env.UUID && parsed.uuid.toLowerCase() !== env.UUID.toLowerCase()) {
      throw new Error("UUID mismatch");
    }

    console.log("CONNECT", parsed.address, parsed.port);

    // پاسخ VLESS دقیقا 2 بایته: [version, addonsLength]
    const header = new Uint8Array([parsed.version, 0]);
    await dial(parsed.address, parsed.port, parsed.payload, header, true);
  }

  // پردازش پیام‌ها به‌صورت ترتیبی (جلوگیری از race condition)
  let chain = Promise.resolve();
  const enqueue = data => {
    chain = chain
      .then(() => onMessage(data))
      .catch(err => {
        console.log("VLESS ERROR", err.message);
        closeAll();
      });
  };

  const early = decodeEarlyData(request.headers.get("sec-websocket-protocol"));
  if (early && early.length) enqueue(early);

  server.addEventListener("message", e => enqueue(e.data));
  server.addEventListener("close", closeAll);
  server.addEventListener("error", closeAll);

  const respHeaders = {};
  const proto = request.headers.get("sec-websocket-protocol");
  if (proto) respHeaders["sec-websocket-protocol"] = proto;

  return new Response(null, { status: 101, webSocket: client, headers: respHeaders });
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (request.method === "OPTIONS") {
      return new Response(null, {
        status: 204,
        headers: {
          "Access-Control-Allow-Origin": "*",
          "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
          "Access-Control-Allow-Headers": "*",
        },
      });
    }

    if (url.pathname === "/") {
      return json({
        success: true,
        gateway: "Trendify Nexus Gateway",
        gateway_id: env.GATEWAY_ID || null,
        region: env.GATEWAY_REGION || DEFAULT_REGION,
        status: "online",
      });
    }

    if (url.pathname === "/health") {
      return json({
        success: true,
        status: "online",
        gateway_id: env.GATEWAY_ID || null,
        region: env.GATEWAY_REGION || DEFAULT_REGION,
        timestamp: new Date().toISOString(),
      });
    }

    if (url.pathname === "/ws" || url.pathname.startsWith("/ws/")) {
      return handleVLESS(request, env);
    }

    return new Response("Not Found", {
      status: 404,
      headers: { "Access-Control-Allow-Origin": "*" },
    });
  },
};
